require "utils"

local grpc = require "grpc"

local g = grpc:new()

g:load([[

// 语法
syntax = "proto3";

// 包名称
package test;

// 服务 1
service human {
  rpc h_add (Object) returns (Object);
  rpc h_find (Object) returns (Object);
}

// 服务 2
service persion {
  rpc p_add (Object) returns (Object);
  rpc p_find (Object) returns (Object);
}

// 消息定义
message Object {
  required string name = 1;
  required int32  age  = 2;
}

]])

local raw = g:encode("test.Object", { name = "車", age = 11 })

local tab = g:decode("test.Object", raw)

var_dump(tab)

-- g:auto_complete("rpc")