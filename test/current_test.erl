-module(current_test).
-include_lib("eunit/include/eunit.hrl").

-export([request_error/3]).

-define(ENDPOINT, <<"localhost:8000">>).
-define(REGION, <<"us-east-1">>).

-define(TABLE, <<"current_test">>).
-define(TABLE_OTHER, <<"current_test_other">>).
-define(i2b(I), list_to_binary(integer_to_list(I))).

-define(NUMBER(I), #{<<"N">> => ?i2b(I)}).

current_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [
      {timeout, 120, ?_test(table_manipulation())},
      {timeout, 30, ?_test(batch_get_write_item())},
      {timeout, 30, ?_test(batch_get_unprocessed_items())},
      {timeout, 30, ?_test(scan())},
      {timeout, 30, ?_test(q())},
      {timeout, 30, ?_test(get_put_update_delete())},
      {timeout, 30, ?_test(retry_with_timeout())},
      {timeout, 30, ?_test(timeout())},
      {timeout, 30, ?_test(throttled())},
      {timeout, 30, ?_test(non_json_error())}
     ]}.


%%
%% DYNAMODB
%%


table_manipulation() ->
    Table = ?TABLE,
    current:delete_table(#{<<"TableName">> => Table}),
    ?assertEqual(ok, current:wait_for_delete(Table, 5000)),

    ?assertMatch({error, {<<"ResourceNotFoundException">>, _}},
                 current:describe_table({[{<<"TableName">>, Table}]})),

    {ok, _} = create_table(Table),

    ?assertEqual(ok, current:wait_for_active(Table, 5000)),
    ?assertMatch({ok, _}, current:describe_table({[{<<"TableName">>, Table}]})),
    %% TODO: list tables and check membership
    ok.


batch_get_write_item() ->
    Table = <<"batch_get_write_item_table">>,
    TableOther = <<"batch_get_write_item_table_other">>,
    {ok, _} = create_table(Table),
    {ok, _} = create_table(TableOther),

    Keys = [#{<<"range_key">> => ?NUMBER(random:uniform(1000)),
              <<"hash_key">> => ?NUMBER(random:uniform(100000))}
            || _ <- lists:seq(1, 50)],

    WriteRequestItems = [#{<<"PutRequest">> => #{<<"Item">> => Key}} || Key <- Keys],
    WriteRequest = #{<<"RequestItems">> => maps:from_list([
            {Table, WriteRequestItems},
            {TableOther, WriteRequestItems}
    ])},

    ?assertEqual(ok, current:batch_write_item(WriteRequest, [])),

    GetRequest = #{<<"RequestItems">> => maps:from_list([
        {Table, #{<<"Keys">> => Keys}},
        {TableOther, #{<<"Keys">> => Keys}}
    ])},

    {ok, Result} = current:batch_get_item(GetRequest),
    ?assertEqual(lists:sort(Keys), lists:sort(maps:get(Table, Result))),
    ?assertEqual(lists:sort(Keys), lists:sort(maps:get(TableOther, Result))).


batch_get_unprocessed_items() ->
    Table = <<"batch_get_unprocessed_items_table">>,
    TableOther = <<"batch_get_unprocessed_items_table_other">>,
    create_table(Table),
    create_table(TableOther),

    Keys = [
        #{
            <<"range_key">> => ?NUMBER(random:uniform(1000)),
            <<"hash_key">>  => ?NUMBER(random:uniform(100000))
        }
        || _ <- lists:seq(1, 150)
    ],

    WriteRequestItems = [
        #{<<"PutRequest">> => #{<<"Item">> => Key}} || Key <- Keys
    ],
    WriteRequest = #{
        <<"RequestItems">> => maps:from_list([
            {Table, WriteRequestItems},
            {TableOther, WriteRequestItems}
        ])
    },

    ?assertEqual(ok, current:batch_write_item(WriteRequest, [])),


    {Keys1, Keys2} = lists:split(110, Keys),
    UnprocessedKeys = {[{Table, {[{<<"Keys">>, Keys2}]}},
                        {TableOther, {[{<<"Keys">>, Keys2}]}}
                       ]},
    meck:new(party, [passthrough]),
    meck:expect(party, post, 4, meck:seq([
        fun (URL, Headers, Body, Opts) ->
            {ok, {{200, <<"OK">>}, ResponseHeaders, ResponseBody}} =
                meck:passthrough([URL, Headers, Body, Opts]),
            Result = jiffy:decode(ResponseBody, [return_maps]),
            % ?assertNot(maps:is_key(<<"UnprocessedKeys">>, Result)),
            MockResult = maps:put(<<"UnprocessedKeys">>, UnprocessedKeys, Result),
            {ok, {{200, <<"OK">>}, ResponseHeaders, jiffy:encode(MockResult)}}
        end,
        meck:passthrough()
    ])),

    GetRequest = #{
        <<"RequestItems">> => maps:from_list([
            {Table, #{<<"Keys">> => Keys1}},
            {TableOther, #{<<"Keys">> => Keys1}}
        ])
    },

    {ok, [{TableOther, Table1}, {Table, Table2}]} =
        current:batch_get_item(GetRequest, []),

    ?assertEqual(lists:sort(Keys), lists:sort(Table1)),
    ?assertEqual(lists:sort(Keys), lists:sort(Table2)),

    ?assert(meck:validate(party)),
    ok = meck:unload(party).


scan() ->
    Table = <<"scan_table">>,
    create_table(Table),
    RequestItems = [
        #{
            <<"PutRequest">> => #{
                <<"Item">> => #{
                    <<"hash_key">> => ?NUMBER(1),
                    <<"range_key">> => ?NUMBER(I),
                    <<"attribute">> => #{<<"S">> => <<"foo">>}
                }
            }
        }
        || I <- lists:seq(1, 100)
    ],
    Request = #{<<"RequestItems">> => maps:from_list([{Table, RequestItems}])},

    ok = current:batch_write_item(Request, []),

    Q = #{<<"TableName">> => Table},

    ?assertMatch({ok, L} when is_list(L), current:scan(Q, [])),

    %% Errors
    ErrorQ = #{<<"TableName">> => <<"non-existing-table">>},
    ?assertMatch({error, {<<"ResourceNotFoundException">>, _}},
                 current:scan(ErrorQ, [])).



take_write_batch_test() ->
    RequestItems = #{<<"table1">> => [1, 2, 3], <<"table2">> => [1, 2, 3]},
    ?assertEqual({RequestItems, #{}}, current:take_write_batch(RequestItems, 25)),

    {Batch1, Rest1} = current:take_write_batch(
                      #{<<"table1">> => lists:seq(1, 30),
                        <<"table2">> => lists:seq(1, 30)}, 25),
    ?assertEqual(#{<<"table1">> => lists:seq(1, 25)}, Batch1),
    ?assertEqual(#{<<"table1">> => lists:seq(26, 30),
                   <<"table2">> => lists:seq(1, 30)}, Rest1),

    {Batch2, Rest2} = current:take_write_batch(Rest1, 25),
    ?assertEqual(#{<<"table1">> => lists:seq(26, 30),
                   <<"table2">> => lists:seq(1, 20)}, Batch2),
    ?assertEqual(#{<<"table2">> => lists:seq(21, 30)}, Rest2),

    {Batch3, Rest3} = current:take_write_batch(Rest2, 25),
    ?assertEqual(#{<<"table2">> => lists:seq(21, 30)}, Batch3),
    ?assertEqual(#{}, Rest3).

take_get_batch_test() ->
    Spec = #{
        <<"Keys">> => [1,2,3],
        <<"AttributesToGet">> => [<<"foo">>, <<"bar">>],
        <<"ConsistentRead">> => false
    },

    {Batch1, Rest1} = current:take_get_batch(#{
        <<"table1">> => Spec,
        <<"table2">> => Spec
    }, 2),

    ?assertEqual(
        #{
            <<"table1">> => #{
                <<"Keys">> => [1, 2],
                <<"AttributesToGet">> => [<<"foo">>, <<"bar">>],
                <<"ConsistentRead">> => false
            }
        },
        Batch1
    ),

    {Batch2, Rest2} = current:take_get_batch(Rest1, 2),
    ?assertEqual(
        #{
            <<"table1">> => #{
                <<"Keys">> => [3],
                <<"AttributesToGet">> => [<<"foo">>, <<"bar">>],
                <<"ConsistentRead">> => false
            },
            <<"table2">> => #{
                <<"Keys">> => [1],
                <<"AttributesToGet">> => [<<"foo">>, <<"bar">>],
                <<"ConsistentRead">> => false
            }
        },
        Batch2
    ),
    {Batch3, Rest3} = current:take_get_batch(Rest2, 2),
    ?assertEqual(
        #{
            <<"table2">> => #{
                <<"Keys">> => [2, 3],
                <<"AttributesToGet">> => [<<"foo">>, <<"bar">>],
                <<"ConsistentRead">> => false
            }
        },
        Batch3
    ),
    ?assertEqual(#{}, Rest3).




q() ->
    Table = <<"q_table">>,
    create_table(Table),

    Items = [#{<<"range_key">> => ?NUMBER(I), <<"hash_key">>  => ?NUMBER(1)}
             || I <- lists:seq(1, 100)],

    RequestItems = [#{<<"PutRequest">> => #{<<"Item">> =>Item}} || Item <- Items],
    Request = #{<<"RequestItems">> => maps:from_list([{Table, RequestItems}])},

    ok = current:batch_write_item(Request, []),

    Q = #{
        <<"TableName">> => Table,
        <<"KeyConditions">> => #{
            <<"hash_key">> => #{
                <<"AttributeValueList">> => [#{<<"N">> => <<"1">>}],
                <<"ComparisonOperator">> => <<"EQ">>
            }
        },
        <<"Limit">> => 10
    },

    {ok, ResultItems} = current:q(Q, []),
    ?assertEqual(lists:sort(Items), lists:sort(ResultItems)),

    %% Count
    CountQ = #{
        <<"TableName">> => Table,
        <<"KeyConditions">> => #{
            <<"hash_key">> => #{
                <<"AttributeValueList">> => [?NUMBER(1)],
                <<"ComparisonOperator">> => <<"EQ">>
            }
        },
        <<"Limit">> => 10,
        <<"Select">> => <<"COUNT">>
    },
    {ok, ResultCount} = current:q(CountQ, []),
    ?assertEqual(100, ResultCount),

    %% Errors
    ErrorQ = #{
        <<"TableName">> => <<"non-existing-table">>,
        <<"KeyConditions">> => #{
            <<"hash_key">> => #{
                <<"AttributeValueList">> => [?NUMBER(1)],
                <<"ComparisonOperator">> => <<"EQ">>
            }
        },
        <<"Limit">> => 10
    },
    ?assertMatch({error, {<<"ResourceNotFoundException">>, _}},
                 current:q(ErrorQ, [])),

    %% Limit
    {ok, LimitedItems} = current:q(Q, [{max_items, 10}]),
    ?assertEqual(10, length(LimitedItems)).



get_put_update_delete() ->
    Table = <<"get_put_update_delete_table">>,
    create_table(Table),

    Key = {[{<<"hash_key">>, {[{<<"N">>, <<"1">>}]}},
            {<<"range_key">>, {[{<<"N">>, <<"1">>}]}}]},

    Item = {[{<<"attribute">>, #{<<"SS">> => [<<"foo">>]}},
             {<<"range_key">>, #{<<"N">>  => <<"1">>}},
             {<<"hash_key">>,  #{<<"N">>  => <<"1">>}}]},

    {ok, NoItem} = current:get_item(#{<<"TableName">> => Table,
                                      <<"Key">> => Key}),
    ?assertNot(maps:is_key(<<"Item">>, NoItem)),


    ?assertMatch({ok, _}, current:put_item(#{<<"TableName">> => Table,
                                             <<"Item">> => Item})),

    {ok, WithItem} = current:get_item(#{<<"TableName">> => Table,
                                        <<"Key">> =>Key}),
    ActualItem = maps:get(<<"Item">>, WithItem),
    ?assertEqual(lists:sort(element(1, Item)), lists:sort(maps:to_list(ActualItem))),

    {ok, _} = current:update_item(#{
        <<"TableName">> => Table,
        <<"AttributeUpdates">> => #{
            <<"attribute">> => #{
                <<"Action">> => <<"ADD">>,
                <<"Value">> => {[{<<"SS">>, [<<"bar">>]}]}
            }
        },
        <<"Key">> => Key
    }),

    {ok, WithUpdate} = current:get_item(#{<<"TableName">> => Table,
                                          <<"Key">> => Key}),
    UpdatedItem = maps:get(<<"Item">>, WithUpdate),
    Attribute = maps:get(<<"attribute">>, UpdatedItem),
    ?assertMatch(#{<<"SS">> := _Values}, Attribute),
    #{<<"SS">> := Values} = Attribute,
    ?assertEqual([<<"bar">>, <<"foo">>], lists:sort(Values)),


    ?assertMatch({ok, _}, current:delete_item(#{<<"TableName">> => Table,
                                                <<"Key">> =>Key})),

    {ok, NoItemAgain} = current:get_item(#{<<"TableName">> => Table,
                                           <<"Key">> => Key}),
    ?assertNot(maps:is_key(<<"Item">>, NoItemAgain)).


retry_with_timeout() ->
    Table = ?TABLE,
    meck:new(party, [passthrough]),
    meck:expect(party, post, fun (_, _, _, _) ->
                                         {error, claim_timeout}
                                 end),

    ?assertEqual({error, max_retries},
                 current:describe_table({[{<<"TableName">>, Table}]},
                                        [{retries, 3}])),

    meck:unload(party).

timeout() ->
    Table = ?TABLE,
    ?assertEqual({error, max_retries},
                 current:describe_table({[{<<"TableName">>, Table}]},
                                        [{call_timeout, 1}])).


throttled() ->
    Table = <<"throttled_table">>,
    create_table(Table),
    ok = clear_table(Table),

    E = <<"com.amazonaws.dynamodb.v20120810#"
          "ProvisionedThroughputExceededException">>,

    ThrottledResponse = {ok, {{400, foo}, [],
                              jiffy:encode(
                                {[{'__type',  E},
                                  {message, <<"foobar">>}]})}},

    meck:new(party, [passthrough]),
    meck:expect(party, post, 4,
                meck_ret_spec:seq(
                  [ThrottledResponse,
                   ThrottledResponse,
                   meck_ret_spec:passthrough()])),

    WriteRequest = #{<<"RequestItems">> => maps:from_list([
        {Table, [#{
            <<"PutRequest">> => #{
                <<"Item">> => #{
                    <<"hash_key">>  => ?NUMBER(1),
                    <<"range_key">> => ?NUMBER(1)
                }
            }
        }]}
    ])},

    ?assertEqual(ok, current:batch_write_item(WriteRequest, [{retries, 3}])),

    meck:unload(party).

non_json_error() ->
    Table = ?TABLE,
    meck:new(party, [passthrough]),
    PartyResponse = {ok, {{413, ""}, [], <<"not a json response!">>}},
    meck:expect(party, post, 4, PartyResponse),

    Key = #{<<"hash_key">> => ?NUMBER(1), <<"range_key">> => ?NUMBER(1)},
    Response = current:get_item(#{<<"TableName">> => Table, <<"Key">> => Key}),

    ?assertEqual({error, {413, <<"not a json response!">>}}, Response),

    meck:unload(party).



%%
%% SIGNING
%%

key_derivation_test() ->
    application:set_env(current, secret_access_key,
                        <<"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY">>),
    application:set_env(current, region, <<"us-east-1">>),
    application:set_env(current, aws_host, <<"iam">>),
    Now = edatetime:datetime2ts({{2012, 2, 15}, {0, 0, 0}}),

    ?assertEqual("f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d",
                 string:to_lower(hmac:hexlify(current:derived_key(Now)))).

post_vanilla_test() ->
    application:set_env(current, region, <<"us-east-1">>),
    application:set_env(current, aws_host, <<"host">>),
    application:set_env(current, access_key, <<"AKIDEXAMPLE">>),
    application:set_env(current, secret_access_key,
                        <<"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY">>),

    Now = edatetime:datetime2ts({{2011, 9, 9}, {23, 36, 0}}),

    %% from post-vanilla.req
    Headers = [{<<"date">>, <<"Mon, 09 Sep 2011 23:36:00 GMT">>},
               {<<"host">>, <<"host.foo.com">>}],

    CanonicalRequest = current:canonical(Headers, ""),
    ?assertEqual(creq("post-vanilla"), iolist_to_binary(CanonicalRequest)),

    HashedCanonicalRequest = string:to_lower(
                               hmac:hexlify(
                                 erlsha2:sha256(CanonicalRequest))),

    ?assertEqual(sts("post-vanilla"),
                 iolist_to_binary(
                   current:string_to_sign(HashedCanonicalRequest, Now))),

    ?assertEqual(authz("post-vanilla"),
                 iolist_to_binary(
                   current:authorization(Headers, "", Now))).



%%
%% HELPERS
%%

creq(Name) ->
    {ok, B} = file:read_file(
                filename:join(["../test", "aws4_testsuite", Name ++ ".creq"])),
    binary:replace(B, <<"\r\n">>, <<"\n">>, [global]).

sts(Name) ->
    {ok, B} = file:read_file(
                filename:join(["../test", "aws4_testsuite", Name ++ ".sts"])),
    binary:replace(B, <<"\r\n">>, <<"\n">>, [global]).


authz(Name) ->
    {ok, B} = file:read_file(
                filename:join(["../test", "aws4_testsuite", Name ++ ".authz"])),
    binary:replace(B, <<"\r\n">>, <<"\n">>, [global]).




setup() ->
    application:start(carpool),
    application:start(party),

    File = filename:join([code:priv_dir(current), "aws_credentials.term"]),
    {ok, Cred} = file:consult(File),
    AccessKey = proplists:get_value(access_key, Cred),
    SecretAccessKey = proplists:get_value(secret_access_key, Cred),

    application:set_env(current, callback_mod, ?MODULE),
    application:set_env(current, endpoint, ?ENDPOINT),
    application:set_env(current, region, ?REGION),
    application:set_env(current, access_key, AccessKey),
    application:set_env(current, secret_access_key, SecretAccessKey),

    ok = party:connect(iolist_to_binary(["http://", ?ENDPOINT]), 2),

    application:start(current).

teardown(_) ->
    application:stop(current).


create_table(Name) ->
    AttrDefs = [
        #{<<"AttributeName">> => <<"hash_key">>, <<"AttributeType">> => <<"N">>},
        #{<<"AttributeName">> => <<"range_key">>, <<"AttributeType">> => <<"N">>}
    ],
    KeySchema = [
        #{<<"AttributeName">> => <<"hash_key">>, <<"KeyType">> => <<"HASH">>},
        #{<<"AttributeName">> => <<"range_key">>, <<"KeyType">> => <<"RANGE">>}
    ],

    R = #{
        <<"AttributeDefinitions">> => AttrDefs,
        <<"KeySchema">> => KeySchema,
        <<"ProvisionedThroughput">> => #{
            <<"ReadCapacityUnits">> => 10,
            <<"WriteCapacityUnits">> => 5
        },
        <<"TableName">> => Name
    },
    {ok, _} = current:create_table(R, [{timeout, 5000}, {retries, 3}]).
    % case current:describe_table(#{<<"TableName">> => Name}) of
    %     {error, {<<"ResourceNotFoundException">>, _}} ->
    %         ?assertMatch({ok, _},
    %                      ),
    %         ok = current:wait_for_active(Name, 5000);
    %     {error, {_Type, Reason}} ->
    %         error_logger:info_msg("~p~n", [Reason]);
    %     {ok, _} ->
    %         ok
    % end.

clear_table(Name) ->
    Query = #{
        <<"TableName">>       => Name,
        <<"AttributesToGet">> => [<<"hash_key">>, <<"range_key">>]
    },
    case current:scan(Query, []) of
        {ok, []} ->
            ok;
        {ok, Items} ->
            RequestItems = [#{<<"DeleteRequest">> => #{<<"Key">> => Item}} || Item <- Items],
            Request = #{<<"RequestItems">> => maps:from_list([{Name, RequestItems}])},
            io:format("~p~n", [Request]),
            ok = current:batch_write_item(Request, []),
            clear_table(Name)
    end.

request_error(Operation, _Start, Reason) ->
    io:format("ERROR in ~p: ~p~n", [Operation, Reason]).
