local pb = require "protobuf"
local pbencode = pb.encode
local pbdecode = pb.decode

local protoc = require "protobuf.protoc"

local grpcio = require "grpc.io"

local lz = require"lz"
local gzcompress = lz.gzcompress
local gzuncompress = lz.gzuncompress

local lfs  = require "lfs"

local type = type
local next = next
local pairs = pairs
local ipairs = ipairs
local assert = assert

local splite = string.gmatch
local spack = string.pack
local sunpack = string.unpack

local service_regex = "service[ ]+([^ %{%}]+)[ ]*{(.-)}"
local service_mathod_regex = "rpc[ ]*([^ ]+)[ ]*%(([^%)%(]+)%)[ ]*returns[ ]*%(([^%)%(]+)%)[ ]*[;]?"

local class = require "class"

local grpc = class("class")

function grpc:ctor(opt)
  self.version = "0.1"
  self.compressed = false
  self.protoc = protoc:new()
  if type(opt) == 'table' then
    self.compressed = opt.compressed and 0x01
  end
  self.services = {}
end

-- 加载protobuf协议
function grpc:load(proto)
  if self.protoc:load(proto) then
    for service_prefix, service_list in splite(proto, service_regex) do
      self.services[service_prefix] = assert(not self.services[service_prefix] and {}, '`' .. service_prefix .. "` is repeatedly defined")
      for service_name, req_name, resp_name in splite(service_list, service_mathod_regex) do
        self.services[service_prefix][service_name] = { req_name = req_name, resp_name = resp_name }
      end
    end 
  end
  return true
end

-- 自动生成代码
function grpc:auto_complete(fordel)
  -- 没有任何协议定义的`服务`
  assert(next(self.services), "The protocol file that supports the service definition is not detected.")
  lfs.mkdir(fordel)
  for key, info in pairs(self.services) do
    grpcio.toFile(fordel, key, info)
  end
  return true
end

-- 编码
function grpc:encode(message_name, message_table)
  assert(type(message_name) == 'string' and type(message_table) == 'table', "Invalid GRPC `message_name` or `message_table`")
  local pbmsg = pbencode(message_name, message_table)
  if self.compressed then
    pbmsg = gzcompress(pbmsg)
  end
  return spack(">BI4", self.compressed or 0x00, #pbmsg) .. pbmsg
end

-- 解码
function grpc:decode(message_name, rawdata)
  assert(type(message_name) == 'string' and type(rawdata) == 'string', "Invalid GRPC `message_name` or `rawdata`")
  local tab
  local compressed, len, index = sunpack(">BI4", rawdata)
  if compressed ~= 0x00 then
    tab = pbdecode(message_name, gzuncompress(rawdata:sub(index)))
  else
    tab = pbdecode(message_name, rawdata:sub(index))
  end
  return tab
end

return grpc