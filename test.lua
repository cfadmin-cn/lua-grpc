require "utils"

local cf = require"cf"

local grpc = require "lua-grpc.io"

local g = grpc:new( {
  -- compressed = true,
  domain = "http://localhost/"
})

g:load([[
  syntax = "proto3";
  package info;
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

cf.fork(function()
  local ret, err = g:call("Greeter", "SayHello", { name = "車先生" }, 5 --[[ 重试时间(秒) ]])
  if not ret then
    return print(ret, err)
  end
  var_dump(ret)
end)

cf.fork(function()
  local ret, err = g:call("Greeter", "SayHello", { name = "車爪鱼" }, 10 --[[ 重试时间(秒) ]])
  if not ret then
    return print(ret, err)
  end
  var_dump(ret)
end)

return require "cf".wait()