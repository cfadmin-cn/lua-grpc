require "utils"

local cf = require "cf"

local client = require "lua-grpc.client"

local gclient = client:new { domain = "http://localhost/", max = 1 }

gclient:load([[
  syntax = "proto3";
  service Greeter {
    rpc SayHello(HelloRequest) returns (HelloReply);
    rpc SayHelloAgain(HelloRequest) returns (HelloReply);
  }
  
  message HelloRequest {
    string name = 1;
  }
  message HelloReply {
    string message = 1;
  }
]])

cf.fork(function ()
  while true do
    local info, err = gclient:call("Greeter", "SayHello", { name = "車先生" })
    if not info then
      return print(false, err)
    end
    var_dump(info)
    cf.sleep(math.random())
  end
end)

cf.fork(function ()
  while true do
    local info, err = gclient:call("Greeter", "SayHelloAgain", { name = "車爪鱼" })
    if not info then
      return print(false, err)
    end
    var_dump(info)
    cf.sleep(math.random())
  end
end)
