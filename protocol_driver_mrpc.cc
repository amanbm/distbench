// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "protocol_driver_mrpc.h"
#include "distbench_netutils.h"
#include "distbench_utils.h"

#include <arpa/inet.h>
#include <sys/mman.h>
#include <thread>
#include <chrono>

#include "distbench_thread_support.h"

namespace distbench {

///////////////////////////////////
// ProtocolDriverHoma Methods //
///////////////////////////////////

ProtocolDriverMRPC::ProtocolDriverMRPC() {}

absl::Status ProtocolDriverMRPC::Initialize(
    const ProtocolDriverOptions& pd_opts, int* port) {
  if (pd_opts.has_netdev_name()) {
    netdev_name_ = pd_opts.netdev_name();
  }

  for (const auto& setting : pd_opts.server_settings()) {
    return absl::InvalidArgumentError(
        absl::StrCat("unknown protocol driver option: ", setting.name()));
  }

  auto maybe_ip = IpAddressForDevice(netdev_name_, pd_opts.ip_version());
  if (!maybe_ip.ok()) return maybe_ip.status();
  server_ip_address_ = maybe_ip.value();

  server_ = bind_mrpc_server(server_ip_address_.toString());
  RunRegisteredThread("MrpcServer", [server_, start_client_, handler_set_]() { 
    handler_set_.WaitForNotification();
    start_client_.TryToNotify();
    local_server_serve(server); 
    LOG(ERROR) << "server stopped" << std::endl;
  });

  RunRegisteredThread("MrpcClient", [client_, start_client_]() { 
    start_client_.WaitForNotification();

    // let server start
    std::this_thread::sleep_for (std::chrono::seconds(1));
    client_ = incrementer_client_connect(server_ip_address_.ToString());
  });

  return absl::OkStatus();
}

ProtocolDriverMRPC::~ProtocolDriverMRPC() {
  ShutdownServer();
  ShutdownClient();
}

void ProtocolDriverMRPC::SetHandler(
  std::function<std::function<void()>(ServerRpcState* state)> handler) {

  auto service_handler = [handler](const RValueRequest *req) -> WValueReply* {
    // set up handler before local serve is called
    GenericRequest* request = new GenericRequest;
    int msg_length = rvaluerequest_key_size(req);
    char rx_buf[1048576];

    // populate generic req with the req
    char* curr = rx_buf;
    for (int i = 0; i < msg_length; i++) {
        *curr = rvaluerequest_key(req, i);
        curr++;
    }
    if (!request->ParseFromArray(rx_buf, msg_length)) {
      LOG(ERROR) << "rx_buf did not parse as a GenericRequest";
    }
    ServerRpcState* rpc_state = new ServerRpcState;
    rpc_state->request = request;
    rpc_state->SetFreeStateFunction([=]() {
      delete rpc_state->request;
      delete rpc_state;
    });

    rpc_state->SetSendResponseFunction([]() {
        LOG(INFO) << "send resp func called" << std::endl;
    });

    handler(rpc_state)();

    std::string response;
    rpc_state->response.SerializeToString(&response);
    const char* const buf = response.data();
    const size_t buflen = response.size();

    WValueReply* rep = new_wvalueresponse();
    const char* currChar = buf;
    for (uint32_t i = 0; i < buflen; i++) {
      wvalueresponse_key_add_byte(rep, (uint8_t) *currChar);
      currChar++;
    }

    return rep;
  };

  CPPIncrementer incr;
  incr.increment_impl = service_handler;
  add_incrementer_service(server, incr);
  handler_set_.TryToNotify();
}

void ProtocolDriverMRPC::SetNumPeers(int num_peers) {
  peer_addresses_.resize(num_peers);
}

absl::Status ProtocolDriverHoma::HandleConnect() {
  return absl::OkStatus();
}

absl::StatusOr<std::string> ProtocolDriverHoma::HandlePreConnect(
    std::string_view remote_connection_info, int peer) {
  ServerAddress addr;
  addr.set_ip_address(server_ip_address_.ip());
  addr.set_port(server_port_);
  addr.set_socket_address(my_server_socket_address_);
  std::string ret;
  addr.AppendToString(&ret);
  return ret;
}

std::vector<TransportStat> ProtocolDriverHoma::GetTransportStats() {
  return {};
}

void ProtocolDriverHoma::ChurnConnection(int peer) {
  // Not required for Homa.
}

void ProtocolDriverHoma::ShutdownServer() {
    // TODO
}

void ProtocolDriverMRPC::ShutdownClient() {
    // TODO
}

void ProtocolDriverHoma::InitiateRpc(int peer_index, ClientRpcState* state,
                                     std::function<void(void)> done_callback) {
  PendingMRPC* new_rpc = new PendingMRPC;

  ++pending_rpcs_;
  new_rpc->done_callback = done_callback;
  new_rpc->state = state;
  new_rpc->request = std::move(state->request);
  new_rpc->serialized_request = "?"; 
  state->request.SerializeToString(&new_rpc->serialized_request);
  const char* const buf = new_rpc->serialized_request.data();
  const size_t buflen = new_rpc->serialized_request.size();

  WValueRequest* req = new_wvaluerequest();
  const char* currChar = buf;
  for (uint32_t i = 0; i < buflen; i++) {
    wvaluerequest_key_add_byte(req, (uint8_t) *currChar);
    currChar++;
  }

  auto callback_fct = [new_rpc, done_callback](const RValueReply *reply) {
      int msg_length = rvalueresponse_key_size(reply);
      // get rx buf
      char rx_buf[1048576];
      // populate generic req with the req
      char* curr = rx_buf;
      for (int i = 0; i < msg_length; i++) {
          *curr = (char) rvalueresponse_key(req, i);
          curr++;
      }

      bool success = new_rpc->response.ParseFromArray(rx_buf, msg_length);
      if (!success) {
        LOG(ERROR) << "rx_buf did not parse as a GenericResponse";
      } else {
        new_rpc->state->request = std::move(new_rpc->request);
        new_rpc->state->response = std::move(new_rpc->response);
      }
      new_rpc->state->success = success;
      --pending_rpcs_;
      done_callback();
      delete new_rpc;
  };

  increment(client_, req, callback_fct);

#ifdef THREAD_SANITIZER
  __tsan_release(new_rpc);
#endif
}

}  // namespace distbench
