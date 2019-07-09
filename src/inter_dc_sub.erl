%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% InterDC subscriber - connects to remote PUB sockets and listens to a defined subset of messages.
%% The messages are filter based on a binary prefix.

-module(inter_dc_sub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("antidote_channels/include/antidote_channel.hrl").

%% API
-export([
    add_dc/2,
    del_dc/1
]).

%% Server methods
-export([
    init/1,
    start_link/0,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% State
-record(state, {
    channel :: pid(),
    listening_dcs
}).

%%%% API --------------------------------------------------------------------+

%% TODO: persist added DCs in case of a node failure, reconnect on node restart.
-spec add_dc(dcid(), [socket_address()]) -> ok.
add_dc(DCId, Publishers) -> gen_server:call(?MODULE, {add_dc, DCId, Publishers}, ?COMM_TIMEOUT).

-spec del_dc(dcid()) -> ok.
del_dc(DCId) -> gen_server:call(?MODULE, {del_dc, DCId}, ?COMM_TIMEOUT).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) ->
    {ok, #state{listening_dcs = sets:new()}}.

%TODO: Doesn't need to create new channels every time. But cannot create on init, because ring is not stable.
handle_call({add_dc, DCId, _Publishers}, _From, #state{channel = Channel, listening_dcs = DCSet} = State) ->
    {ok, Host} = inet:parse_address(application:get_env(antidote, rabbitmq_host, ?DEFAULT_RABBITMQ_HOST)),
    Port = application:get_env(antidote, rabbitmq_port, ?DEFAULT_RABBITMQ_PORT),

    delete_chan(Channel),
    {ok, NewChannel} = case antidote_channel:is_alive(channel_rabbitmq, #rabbitmq_network{host = Host, port = Port}) of
                           true ->
                               PartString = lists:map(
                                   fun(P) ->
                                       list_to_binary(io_lib:format("~p", [P]))
                                   end, dc_utilities:get_my_partitions()),
                               Config = #{
                                   module => channel_rabbitmq,
                                   pattern => pub_sub,
                                   handler => self(),
                                   topics => PartString,
                                   namespace => <<>>,
                                   network_params => #{}
                               },
                               antidote_channel:start_link(Config);
                           _ -> {stop, {error_connecting}}
                       end,
    {reply, ok, State#state{channel = NewChannel, listening_dcs = sets:add_element(DCId, DCSet)}};

handle_call({del_dc, {DcID, _}}, _From, #state{listening_dcs = DCSet} = State) ->
    {reply, ok, State#state{listening_dcs = sets:del_element(DcID, DCSet)}}.

handle_cast(_Request, State) -> {noreply, State}.

handle_info(#pub_sub_msg{payload = #interdc_txn{dcid = DCId} = Msg}, #state{listening_dcs = DCs} = State) ->
    case sets:is_element(DCId, DCs) of
        true ->
            inter_dc_sub_vnode:deliver_txn(Msg);
        _ -> ok % Ignore local messages
    end,
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, #state{channel = Channel}) ->
    antidote_channel:stop(Channel).

delete_chan(undefined) -> ok;

delete_chan(Channel) ->
    antidote_channel:stop(Channel).
