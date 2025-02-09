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

#ifndef DISTBENCH_PROTOCOL_DRIVER_MRPC_H_
#define DISTBENCH_PROTOCOL_DRIVER_MRPC_H_

#include <thread>

#include "distbench_netutils.h"
#include "distbench_utils.h"
#include "protocol_driver.h"

// TODO include mrpc stub file

namespace distbench {

struct PendingMRPC {
  ClientRpcState* state;
  GenericRequest request;
  std::string serialized_request;
  std::function<void(void)> done_callback;
};

class ProtocolDriverMRPC: public ProtocolDriver {
 public:
  ProtocolDriverMRPC();

  ~ProtocolDriverMRPC() override;

  absl::Status Initialize(const ProtocolDriverOptions& pd_opts,
                          int* port) override;

  void SetHandler(std::function<std::function<void()>(ServerRpcState* state)>
                      handler) override;

  void SetNumPeers(int num_peers) override;

  absl::Status HandleConnect(std::string remote_connection_info, int peer) override;

  absl::StatusOr<std::string> HandlePreConnect(
      std::string_view remote_connection_info, int peer) override;

  std::vector<TransportStat> GetTransportStats() override;

  void InitiateRpc(int peer_index, ClientRpcState* state,
                   std::function<void(void)> done_callback) override;

  void ChurnConnection(int peer) override;

  void ShutdownServer() override;

  void ShutdownClient() override;

 private:
  void ClientCompletionThread();
  void ServerThread();


  int server_port_ = 0;
  DeviceIpAddress server_ip_address_;
  std::string my_server_socket_address_;

  IncrementerClient* client_;

  LocalServer* server_;

  std::atomic<int> pending_rpcs_ = 0;

  std::string netdev_name_;
  std::thread client_completion_thread_;
  std::thread server_thread_;
  SafeNotification handler_set_;
  SafeNotification start_client_;
  SafeNotification shutting_down_server_;
  SafeNotification shutting_down_client_;

  std::function<std::function<void()>(ServerRpcState* state)> rpc_handler_;

};

}  // namespace distbench

#endif  // DISTBENCH_PROTOCOL_DRIVER_MRPC_H_
