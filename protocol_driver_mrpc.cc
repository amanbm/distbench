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

#include "distbench_thread_support.h"

namespace distbench {

///////////////////////////////////
// ProtocolDriverHoma Methods //
///////////////////////////////////

ProtocolDriverHoma::ProtocolDriverHoma() {}

absl::Status ProtocolDriverHoma::Initialize(
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

  service_ = new CPPIncrementer;
  server_ = bind_mrpc_server(server_ip_address_.toString());
  add_incrementer_service(server, *service);
  RunRegisteredThread("MrpcServer", [server]() { local_server_serve(server); });

  client_ = incrementer_client_connect(server_ip_address_.ToString());

  return absl::OkStatus();
}

ProtocolDriverMRPC::~ProtocolDriverMRPC() {
  ShutdownServer();
  ShutdownClient();
}

void ProtocolDriverHoma::SetHandler(
    std::function<std::function<void()>(ServerRpcState* state)> handler) {

  incr.increment_impl = increment;
}

void ProtocolDriverHoma::SetNumPeers(int num_peers) {
  peer_addresses_.resize(num_peers);
}

absl::Status ProtocolDriverHoma::HandleConnect(
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

  new_rpc->done_callback = done_callback;
  new_rpc->state = state;
  new_rpc->serialized_request = "?";  // Homa can't send a 0 byte message :(
  state->request.AppendToString(&new_rpc->serialized_request);
  const char* const buf = new_rpc->serialized_request.data();
  const size_t buflen = new_rpc->serialized_request.size();

  WValueRequest* req = new_wvaluerequest();
  wvaluerequest_set_val(req, pending_rpcs_);
  const char* currChar = buf;
  for (uint32_t i = 0; i < buflen; i++) {
    wvaluerequest_key_add_byte(req, (uint8_t) *currChar);
    currChar++;
  }

 auto callback_fct = [this, done_callback](const RValueReply *reply) {
    done_callback();
    LOG(INFO) << "response: ValueReply { val: " << rvaluereply_val(reply) << " }" << std::endl;
    delete new_rpc;
    --pending_rpcs_;
  };

  increment(client_, req, callback_fct);

#ifdef THREAD_SANITIZER
  __tsan_release(new_rpc);
#endif

  ++pending_rpcs_;
}

void ProtocolDriverHoma::ServerThread() {
  std::atomic<int> pending_actionlist_threads = 0;

  handler_set_.WaitForNotification();
  while (1) {
    errno = 0;
    ssize_t msg_length = server_receiver_->receive(HOMA_RECVMSG_REQUEST, 0);
    if (shutting_down_server_.HasBeenNotified()) {
      break;
    }
    int recv_errno = errno;
    if (msg_length < 0) {
      if (recv_errno != EINTR && recv_errno != EAGAIN) {
        LOG(ERROR) << "server homa_recv had an error: " << strerror(recv_errno);
      }
      continue;
    }
    if (msg_length == 0) {
      LOG(ERROR) << "server homa_recv got zero length request.";
      continue;
    }
    CHECK(server_receiver_->is_request());
    const sockaddr_in_union src_addr = *server_receiver_->src_addr();
    const uint64_t rpc_id = server_receiver_->id();

    GenericRequest* request = new GenericRequest;
    char rx_buf[1048576];
    server_receiver_->copy_out((void*)rx_buf, 0, sizeof(rx_buf));
    if (!request->ParseFromArray(rx_buf + 1, msg_length - 1)) {
      LOG(ERROR) << "rx_buf did not parse as a GenericRequest";
    }
    ServerRpcState* rpc_state = new ServerRpcState;
    rpc_state->request = request;
    rpc_state->SetFreeStateFunction([=]() {
      delete rpc_state->request;
      delete rpc_state;
    });
    rpc_state->SetSendResponseFunction([=, &pending_actionlist_threads]() {
      std::string txbuf = "!";  // Homa can't send a 0 byte message :(
      rpc_state->response.AppendToString(&txbuf);
      int64_t error = homa_reply(homa_server_sock_, txbuf.c_str(),
                                 txbuf.length(), &src_addr, rpc_id);
      if (error) {
        LOG(ERROR) << "homa_reply for " << rpc_id
                   << " returned error: " << strerror(errno);
      }
      --pending_actionlist_threads;
    });
    auto fct_action_list_thread = rpc_handler_(rpc_state);
    ++pending_actionlist_threads;
    if (fct_action_list_thread)
      RunRegisteredThread("DedicatedActionListThread", fct_action_list_thread)
          .detach();
  }
  while (pending_actionlist_threads) {
    sched_yield();
  }
}

void ProtocolDriverHoma::ClientCompletionThread() {
  while (!shutting_down_client_.HasBeenNotified() || pending_rpcs_) {
    errno = 0;
    ssize_t msg_length = client_receiver_->receive(HOMA_RECVMSG_RESPONSE, 0);
    int recv_errno = errno;
    if (msg_length < 0) {
      if (recv_errno != EINTR && recv_errno != EAGAIN) {
        LOG(ERROR) << "homa_recv had an error: " << strerror(recv_errno);
      }
      continue;
    }

    PendingHomaRpc* pending_rpc = reinterpret_cast<PendingHomaRpc*>(
        client_receiver_->completion_cookie());
#ifdef THREAD_SANITIZER
    __tsan_acquire(pending_rpc);
#endif
    CHECK(pending_rpc) << "Completion cookie was NULL";
    if (recv_errno || !msg_length) {
      pending_rpc->state->success = false;
    } else {
      pending_rpc->state->success = true;
      char rx_buf[1048576];
      CHECK(!client_receiver_->is_request());
      client_receiver_->copy_out((void*)rx_buf, 0, sizeof(rx_buf));
      if (!pending_rpc->state->response.ParseFromArray(rx_buf + 1,
                                                       msg_length - 1)) {
        LOG(ERROR) << "rx_buf did not parse as a GenericResponse";
      }
    }
    pending_rpc->done_callback();
    --pending_rpcs_;
    delete pending_rpc;
  }
}

}  // namespace distbench
