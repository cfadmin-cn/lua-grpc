require "utils"

local server = require "lua-grpc.server"

local s = server:new()

s:load([[
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

s:service('Greeter', 'SayHello', function (headers, obj)
  var_dump(headers)
  return {
    message = "Hello " .. obj.name
  }
end)

s:service('Greeter', 'SayHelloAgain', function (headers, obj)
  var_dump(headers)
  return {
    message = "Hello " .. obj.name
  }
end)

s:listen("localhost", 80)

s:run()