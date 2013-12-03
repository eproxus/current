%% @doc: DynamoDB client
-module(current).
-compile(export_all).


%%
%% LOW-LEVEL API
%%


batch_get_item(Request, Opts)   -> do_batch_get_item(Request, Opts).
batch_write_item(Request, Opts) -> do_batch_write_item(Request, Opts).
create_table(Request, Opts)     -> retry(create_table, Request, Opts).
delete_item(Request)            -> retry(delete_item, Request, []).
delete_item(Request, Opts)      -> retry(delete_item, Request, Opts).
delete_table(Request)           -> retry(delete_table, Request, []).
delete_table(Request, Opts)     -> retry(delete_table, Request, Opts).
describe_table(Request)         -> retry(describe_table, Request, []).
describe_table(Request, Opts)   -> retry(describe_table, Request, Opts).
get_item(Request)               -> retry(get_item, Request, []).
get_item(Request, Opts)         -> retry(get_item, Request, Opts).
list_tables(Request)            -> retry(list_tables, Request, []).
list_tables(Request, Opts)      -> retry(list_tables, Request, Opts).
put_item(Request)               -> retry(put_item, Request, []).
put_item(Request, Opts)         -> retry(put_item, Request, Opts).
q(Request, Opts)                -> do_query(Request, Opts).
scan(Request, Opts)             -> do_scan(Request, Opts).
update_item(Request)            -> retry(update_item, Request, []).
update_item(Request, Opts)      -> retry(update_item, Request, Opts).
update_table(Request)           -> retry(update_table, Request, []).
update_table(Request, Opts)     -> retry(update_table, Request, Opts).



%%
%% HIGH-LEVEL HELPERS
%%

wait_for_active(Table, Timeout) ->
    case describe_table({[{<<"TableName">>, Table}]}, [{timeout, Timeout}]) of
        {ok, {[{<<"Table">>, {Description}}]}} ->
            case proplists:get_value(<<"TableStatus">>, Description) of
                <<"ACTIVE">> ->
                    ok;
                <<"DELETING">> ->
                    {error, deleting};
                _Other ->
                    wait_for_active(Table, Timeout)
            end;
        {error, {<<"ResourceNotFoundException">>, _}} ->
            {error, not_found}
    end.


wait_for_delete(Table, Timeout) ->
    case describe_table({[{<<"TableName">>, Table}]}, [{timeout, Timeout}]) of
        {ok, {[{<<"Table">>, {Description}}]}} ->
            case proplists:get_value(<<"TableStatus">>, Description) of
                <<"DELETING">> ->
                    wait_for_delete(Table, Timeout);
                Other ->
                    {error, {unexpected_state, Other}}
            end;
        {error, {<<"ResourceNotFoundException">>, _}} ->
            ok
    end.


%% ============================================================================
%% IMPLEMENTATION
%% ============================================================================




%%
%% BATCH GET AND WRITE
%%


do_batch_get_item(Request, Opts) ->
    do_batch_get_item({Request}, [], Opts).

do_batch_get_item({Request}, Acc, Opts) ->

    {value, {<<"RequestItems">>, RequestItems}, CleanRequest} =
        lists:keytake(<<"RequestItems">>, 1, Request),

    {Batch, Rest} = take_batch(RequestItems, 100),
    BatchRequest = {[{<<"RequestItems">>, {Batch}} | CleanRequest]},

    case retry(batch_write_item, BatchRequest, Opts) of
        {ok, {Result}} ->
            {Responses} = proplists:get_value(<<"Responses">>, Result),
            NewAcc = Responses ++ Acc,

            {Unprocessed} = proplists:get_value(<<"UnprocessedKeys">>, Result),
            case Unprocessed =:= [] andalso Rest =:= [] of
                true ->
                    NewAcc;
                false ->
                    Remaining = orddict:merge(fun (_, Left, Right) ->
                                                      Left ++ Right
                                              end,
                                              orddict:from_list(Unprocessed),
                                              orddict:from_list(Rest)),

                    do_batch_get_item({[{<<"RequestItems">>, {Remaining}}]},
                                      NewAcc, Opts)
            end;
        {error, _} = Error ->
            Error
    end.


do_batch_write_item({Request}, Opts) ->
    {value, {<<"RequestItems">>, RequestItems}, CleanRequest} =
        lists:keytake(<<"RequestItems">>, 1, Request),

    {Batch, Rest} = take_batch(RequestItems, 25),
    error_logger:info_msg("~p~n", [Batch]),
    BatchRequest = {[{<<"RequestItems">>, {Batch}} | CleanRequest]},

    case retry(batch_write_item, BatchRequest, Opts) of
        {ok, {Result}} ->
            {Unprocessed} = proplists:get_value(<<"UnprocessedItems">>, Result),
            case Unprocessed =:= [] andalso Rest =:= [] of
                true ->
                    ok;
                false ->
                    Remaining = orddict:merge(fun (_, Left, Right) ->
                                                      Left ++ Right
                                              end,
                                              orddict:from_list(Unprocessed),
                                              orddict:from_list(Rest)),

                    do_batch_write_item({[{<<"RequestItems">>, {Remaining}}]}, Opts)
            end;
        {error, _} = Error ->
            Error
    end.


take_batch({RequestItems}, MaxItems) ->
    %% TODO: Validate item size
    %% TODO: Chunk on 1MB request size
    do_take_batch(RequestItems, 0, MaxItems, []).

do_take_batch(Remaining, MaxItems, MaxItems, Acc) ->
    {lists:reverse(Acc), Remaining};

do_take_batch([], _, _, Acc) ->
    {lists:reverse(Acc), []};

do_take_batch([{Table, Requests} | RemainingTables], N, MaxItems, Acc) ->
    case split_batch(MaxItems, Requests, []) of
        {Batch, []} ->
            do_take_batch(RemainingTables,
                          N + length(Batch),
                          MaxItems,
                          [{Table, Batch} | Acc]);
        {Batch, Rest} ->
            do_take_batch([{Table, Rest} | RemainingTables],
                          N + length(Batch),
                          MaxItems,
                          [{Table, Batch} | Acc])
    end.



split_batch(0, T, Acc)       -> {lists:reverse(Acc), T};
split_batch(_, [], Acc)      -> {[], Acc};
split_batch(_, [H], Acc)     -> {lists:reverse([H | Acc]), []};
split_batch(N, [H | T], Acc) -> split_batch(N-1, T, [H | Acc]).





%%
%% QUERY
%%

do_query(Request, Opts) ->
    do_query(Request, [], Opts).

do_query({UserRequest}, Acc, Opts) ->
    ExclusiveStartKey = case proplists:get_value(<<"ExclusiveStartKey">>, UserRequest) of
                            undefined ->
                                [];
                            StartKey ->
                                [{<<"ExclusiveStartKey">>, StartKey}]
                        end,

    Request = {ExclusiveStartKey ++ UserRequest},

    case retry('query', Request, Opts) of
        {ok, {Response}} ->
            Items = proplists:get_value(<<"Items">>, Response),
            case proplists:get_value(<<"LastEvaluatedKey">>, Response) of
                undefined ->
                    {ok, Items ++ Acc};
                LastEvaluatedKey ->
                    NextRequest = {lists:keystore(
                                     <<"ExclusiveStartKey">>, 1,
                                     UserRequest,
                                     {<<"ExclusiveStartKey">>, LastEvaluatedKey})},
                    do_query(NextRequest, Items ++ Acc, Opts)
            end
    end.







%%
%% SCAN
%%


do_scan(Request, Opts) ->
    do_scan(Request, [], Opts).

do_scan({UserRequest}, Acc, Opts) ->
    ExclusiveStartKey = case proplists:get_value(<<"ExclusiveStartKey">>, UserRequest) of
                            undefined ->
                                [];
                            StartKey ->
                                [{<<"ExclusiveStartKey">>, StartKey}]
                        end,

    Request = {ExclusiveStartKey ++ UserRequest},

    case retry(scan, Request, Opts) of
        {ok, {Response}} ->
            Items = proplists:get_value(<<"Items">>, Response),
            case proplists:get_value(<<"LastEvaluatedKey">>, Response) of
                undefined ->
                    {ok, Items ++ Acc};
                LastEvaluatedKey ->
                    NextRequest = {lists:keystore(
                                     <<"ExclusiveStartKey">>, 1,
                                     UserRequest,
                                     {<<"ExclusiveStartKey">>, LastEvaluatedKey})},
                    do_scan(NextRequest, Items ++ Acc, Opts)
            end
    end.







%%
%% INTERNALS
%%

retry(Op, Request, Opts) ->
    case proplists:is_defined(no_retry, Opts) of
        true ->
            do(Op, Request, timeout(Opts));
        false ->
            retry(Op, Request, 0, os:timestamp(), Opts)
    end.

retry(Op, Request, Retries, Start, Opts) ->
    RequestStart = os:timestamp(),
    case do(Op, Request, timeout(Opts)) of
        {ok, Response} ->

            case proplists:get_value(<<"ConsumedCapacity">>,
                                     element(1, Response)) of
                undefined -> ok;
                Capacity  -> catch (callback_mod()):request_complete(
                                     Op, RequestStart, Capacity)
            end,

            {ok, Response};

        {error, Reason} = Error ->
            Retry = case Reason of
                        {<<"ProvisionedThroughputExceededException">>, _} -> true;
                        {<<"ResourceNotFoundException">>, _}              -> false;
                        {<<"ResourceInUseException">>, _}                 -> true;
                        {<<"ValidationException">>, _}                    -> false;
                        timeout                                           -> true
                    end,
            case Retry of
                true ->
                    case Retries+1 =:= retries(Opts) orelse
                        timer:now_diff(os:timestamp(), Start) / 1000 > timeout(Opts) of
                        true ->
                            {error, max_retries};
                        false ->
                            timer:sleep(trunc(math:pow(2, Retries) * 50)),
                            retry(Op, Request, Retries+1, Start, Opts)
                    end;
                false ->
                    Error
            end
    end.



do(Operation, {UserRequest}, Timeout) ->
    Now = edatetime:now2ts(),

    Request = {lists:keystore(<<"ReturnConsumedCapacity">>, 1, UserRequest,
                              {<<"ReturnConsumedCapacity">>, <<"TOTAL">>})},

    Body = jiffy:encode(Request),

    URL = "http://dynamodb." ++ endpoint() ++ ".amazonaws.com/",
    Headers = [
               {"host", "dynamodb." ++ endpoint() ++ ".amazonaws.com"},
               {"content-type", "application/x-amz-json-1.0"},
               {"x-amz-date", binary_to_list(edatetime:iso8601(Now))},
               {"x-amz-target", target(Operation)}
              ],

    Signed = [{"Authorization", authorization(Headers, Body, Now)} | Headers],

    case lhttpc:request(URL, "POST", Signed, Body, Timeout) of
        {ok, {{200, "OK"}, _, ResponseBody}} ->
            {ok, jiffy:decode(ResponseBody)};

        {ok, {{Code, _}, _, ResponseBody}}
          when 400 =< Code andalso Code =< 499 ->
            {Response} = jiffy:decode(ResponseBody),
            Type = case proplists:get_value(<<"__type">>, Response) of
                       <<"com.amazonaws.dynamodb.v20120810#", T/binary>> ->
                           T;
                       <<"com.amazon.coral.validate#", T/binary>> ->
                           T;
                       <<"com.amazon.coral.service#", T/binary>> ->
                           T
                   end,
            Message = case proplists:get_value(<<"message">>, Response) of
                          undefined ->
                              %% com.amazon.coral.service#SerializationException
                              proplists:get_value(<<"Message">>, Response);
                          M ->
                              M
                      end,
            {error, {Type, Message}};

        {ok, {{Code, _}, _, ResponseBody}}
          when 500 =< Code andalso Code =< 599 ->
            {error, {server, jiffy:decode(ResponseBody)}};

        {error, Reason} ->
            {error, Reason}
    end.


timeout(Opts) -> proplists:get_value(timeout, Opts, 5000).
retries(Opts) -> proplists:get_value(retries, Opts, 3).


%%
%% AWS4 request signing
%% http://docs.aws.amazon.com/general/latest/gr/signature-version-4.html
%%


authorization(Headers, Body, Now) ->
    CanonicalRequest = canonical(Headers, Body),

    HashedCanonicalRequest = string:to_lower(
                               hmac:hexlify(
                                 erlsha2:sha256(CanonicalRequest))),

    StringToSign = string_to_sign(HashedCanonicalRequest, Now),

    lists:flatten(
      ["AWS4-HMAC-SHA256 ",
       "Credential=", credential(Now), ", ",
       "SignedHeaders=", string:join([string:to_lower(K)
                                      || {K, _} <- lists:sort(Headers)],
                                     ";"), ", ",
       "Signature=", signature(StringToSign, Now)]).


canonical(Headers, Body) ->
    string:join(
      ["POST",
       "/",
       "",
       [string:to_lower(K) ++ ":" ++ V ++ "\n" || {K, V} <- lists:sort(Headers)],
       string:join([string:to_lower(K) || {K, _} <- lists:sort(Headers)],
                   ";"),
       hexdigest(Body)],
      "\n").

string_to_sign(HashedCanonicalRequest, Now) ->
    ["AWS4-HMAC-SHA256", "\n",
     binary_to_list(edatetime:iso8601_basic(Now)), "\n",
     [ymd(Now), "/", endpoint(), "/", aws_host(), "/aws4_request"], "\n",
     HashedCanonicalRequest].


derived_key(Now) ->
    Secret = ["AWS4", secret_key()],
    Date = hmac:hmac256(Secret, ymd(Now)),
    Region = hmac:hmac256(Date, endpoint()),
    Service = hmac:hmac256(Region, aws_host()),
    hmac:hmac256(Service, "aws4_request").


signature(StringToSign, Now) ->
    string:to_lower(
      hmac:hexlify(
        hmac:hmac256(derived_key(Now),
                     StringToSign))).



credential(Now) ->
    [access_key(), "/", ymd(Now), "/", endpoint(), "/", aws_host(), "/aws4_request"].

hexdigest(Body) ->
    string:to_lower(hmac:hexlify(erlsha2:sha256(Body))).



target(batch_write_item) -> "DynamoDB_20120810.BatchWriteItem";
target(create_table)     -> "DynamoDB_20120810.CreateTable";
target(delete_table)     -> "DynamoDB_20120810.DeleteTable";
target(describe_table)   -> "DynamoDB_20120810.DescribeTable";
target(list_tables)      -> "DynamoDB_20120810.ListTables";
target('query')          -> "DynamoDB_20120810.Query";
target(scan)             -> "DynamoDB_20120810.Scan";
target(get_item)         -> "DynamoDB_20120810.GetItem";
target(update_item)      -> "DynamoDB_20120810.UpdateItem";
target(put_item)         -> "DynamoDB_20120810.PutItem";
target(delete_item)      -> "DynamoDB_20120810.DeleteItem";

target(Target)           -> throw({unknown_target, Target}).


%%
%% Callbacks
%%

request_complete(_Operation, _Start, _Capacity) ->
    ok.

%%
%% INTERNAL HELPERS
%%


endpoint() ->
    {ok, Endpoint} = application:get_env(current, endpoint),
    Endpoint.

aws_host() ->
    application:get_env(current, aws_host, "dynamodb").

access_key() ->
    {ok, Access} = application:get_env(current, access_key),
    Access.

secret_key() ->
    {ok, Secret} = application:get_env(current, secret_access_key),
    Secret.

ymd(Now) ->
    {Y, M, D} = edatetime:ts2date(Now),
    io_lib:format("~4.10.0B~2.10.0B~2.10.0B", [Y, M, D]).

callback_mod() ->
    case application:get_env(current, callback_mod) of
        {ok, Mod} ->
            Mod;
        undefined ->
            throw(current_missing_callback_mod)
    end.

