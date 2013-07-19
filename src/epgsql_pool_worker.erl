-module(epgsql_pool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).
-compile([{parse_transform, lager_transform}, debug_info]).

-export([start_link/1, squery/2, equery/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {conn, connect_args}).


% public api

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

squery(Worker, Sql) ->
    gen_server:call(Worker, {squery, Sql}, infinity).

equery(Worker, Stmt, Params) ->
    gen_server:call(Worker, {equery, Stmt, Params}, infinity).


% gen_server callbacks

init(Args) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    Timeout = proplists:get_value(timeout, Args),
    Opts = [{database, Database}, {timeout, Timeout}],
    {ok, #state{connect_args={Hostname, Username, Password, Opts}}}.


%% handle call

handle_call({squery, Stmt}, _From, State) ->
    {Conn, Result} = case connect(State) of
        {ok, Conn1} ->
            {Conn1, do_squery(Conn1, Stmt)};
        {error, _Error} ->
            {undefined, {error, unavailable}}
    end,
    {reply, Result, State#state{conn=Conn}};

handle_call({equery, Stmt, Params}, _From, State) ->
    {Conn, Result} = case connect(State) of
        {ok, Conn1} ->
            {Conn1, do_equery(Conn1, Stmt, Params)};
        {error, _Error} ->
            {undefined, {error, unavailable}}
    end,
    {reply, Result, State#state{conn=Conn}};

handle_call(connect, _From, #state{conn=undefined}=State) ->
    case connect(State) of
        {ok, Conn} ->
            {reply, ok, State#state{conn=Conn}};
        {error, _Error} ->
            {reply, ok, State#state{conn=undefined}}
    end;

handle_call(connect, _From, State) ->
    {reply, connected, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%% handle_cast

handle_cast(_Msg, State) ->
    {noreply, State}.


%% handle_info

handle_info({'EXIT',Conn,sock_closed}, #state{conn=Conn}=State) ->
    {noreply, State#state{conn=undefined}};

handle_info(Info, State) ->
    lager:debug("Info: ~p~nState: ~p~n", [Info, State]),
    {noreply, State}.


%% terminate

terminate(_Reason, #state{conn=undefined}) ->
    ok;

terminate(_Reason, #state{conn=Conn}) ->
    ok = pgsql:close(Conn),
    ok.


%% code_change

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% private functions

connect(#state{conn=Conn}=State) when is_pid(Conn) ->
    case is_process_alive(Conn) of
        true  -> {ok, Conn};
        false -> connect(State#state{conn=undefined})
    end;


connect(#state{connect_args={Hostname, Username, Password, Opts}}) ->
    case pgsql:connect(Hostname, Username, Password, Opts) of
        {ok, Conn} ->
            {ok, Conn};
        {error, Error} ->
            lager:error("connection failed: ~p", [Error]),
            {error, Error}
    end.


do_squery(Conn, Stmt) ->
    try_query(fun() -> pgsql:squery(Conn, Stmt) end).


do_equery(Conn, Stmt, Params) ->
    try_query(fun() -> pgsql:equery(Conn, Stmt, Params) end).


try_query(Fun) ->
    try Fun()
    catch
        error:Error ->
            lager:error("Query failed: ~p", [Error]),
            {error, Error}
    end.
