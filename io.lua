local sys = require "sys"
local new_tab = sys.new_tab

local cf = require "cf"

local LOG = require "logging":new { dumped = true, path = "grpcio"}

local pb = require "protobuf"
local pbencode = pb.encode
local pbdecode = pb.decode

local protoc = require "protobuf.protoc"

local lz = require"lz"
local uncompress = lz.uncompress
local gzuncompress = lz.gzuncompress

local http2 = require "lua-http2.client"

local type = type
local assert = assert

local toint = math.tointeger

local fmt = string.format
local gsub = string.gsub
local match = string.match
local splite = string.gmatch
local spack = string.pack
local sunpack = string.unpack
local lower = string.lower

local remove = table.remove
local insert = table.insert

local service_regex = "service[ ]+([^ %{%}]+)[ ]*{(.-)}"
local service_mathod_regex = "rpc[ ]*([^ ]+)[ ]*%(([^%)%(]+)%)[ ]*returns[ ]*%(([^%)%(]+)%)[ ]*[;]?"

local class = require "class"

local grpcio = class("class")

---@class grpcio
---@field private domain string @连接域名
---@field private port integer @监听端口
function grpcio:ctor(opt)
  self.version = "0.1"
  self.services = {}
  self.pool = new_tab(64, 0)
  self.protoc = protoc:new()
  if type(opt) == "table" then
    self.domain = opt.domain or "http://localhost/"
    self.port = opt.port or 8080
  else
    self.domain = "http://localhost/"
    self.port = 8080
  end
end

---comment 从字符串内容加载protobuf协议
---@param proto string @protobuf内容
---@return grpcio
function grpcio:load(proto)
  proto = gsub(gsub(proto, "%/%/[^%\r%\n]+", ""), "/%*.+%*/", "")
  if self.protoc:load(proto) and self.services then
    local pkg = match(proto, "package ([^ ;]+)[ ;]-") or ""
    if pkg ~= "" then
      pkg = pkg .. "."
    end
    for service, service_list in splite(proto, service_regex) do
      self.services[service] = assert(not self.services[service] and {}, 'WARNING: service[`' .. service .. "`] is repeatedly defined")
      for method, req, resp in splite(service_list, service_mathod_regex) do
        self.services[service][method] = { pkg = pkg, req = req, resp = resp }
      end
    end
  end
  -- var_dump(self.services)
  return self
end

---comment 从给定的文件路径中加载protobuf协议文件
---@param filename string @protobuf文件名
---@return grpcio
function grpcio:loadfile(filename)
  -- 尝试读取文件
  local f = assert(io.open(filename), "r")
  local proto = f:read "*a"
  f:close()
  return self:load(proto)
end

---comment 发起远程调用
---@param service_name string @服务名称
---@param method_name string @方法名称
---@param body table @请求内容
---@param timeout number @重试时间
---@return table<any, any> | nil, string
function grpcio:call(service_name, method_name, body, timeout)
  -- RPC Service
  local obj = assert(self.services[service_name] and self.services[service_name][method_name], "Invalid grpc service or method.")
  local client = remove(self.pool)
  if not client then
    while 1 do
      client = http2:new { domain = self.domain }
      if client:connect() then
        break
      end
      client:close()
      client = nil
      cf.sleep(1)
      LOG:WARN("The grpc client cannot connect to the server! Retrying...")
    end
  end
  local response = client:request(fmt("/%s/%s", service_name, method_name), "POST", { ["te"] = "trailers", ["content-type"] = "application/grpc", ["grpc-accept-encoding"] = "gzip,identity" }, self:encode(obj.pkg .. obj.req, body), timeout)
  if type(response) ~= 'table' then
    client:close(); client = nil;
    return self:call(service_name, method_name, body, timeout)
  end
  -- var_dump(response)
  insert(self.pool, client)
  -- 服务器发生错误的时候.
  if toint(response["headers"]["grpc-status"]) ~= 0 then
    return nil, response["headers"]["grpc-message"] or "Unknown grpc error."
  end
  -- 如果不是GRPC响应类型
  local content_type = lower(response["headers"]["content-type"] or "")
  if content_type ~= "application/grpc" and content_type ~= "application/grpc+proto" then
    return nil, "Invalid grpc server content-type : " .. (response["headers"]["content-type"] or "Unknown grpc content-type.")
  end
  return self:decode(obj.pkg .. obj.resp, response.body, response["headers"]["grpc-encoding"])
end

---comment `GRPC`的序列化方法
---@param message_name string @package与message name
---@param message_table table @待序列化的table
---@return string @序列化成功将返回table
function grpcio:encode(message_name, message_table)
  assert(type(message_name) == 'string' and type(message_table) == 'table', "Invalid GRPC `message_name` or `message_table`")
  local pbmsg = pbencode(message_name, message_table)
  return spack(">BI4", 0x00, #pbmsg) .. pbmsg
end

---comment `GRPC`的反序列化方法
---@param message_name string @package与message name
---@param rawdata string @待反序列化的string
---@param compressed string | nil @`nil`表示不需要未压缩, `gzip`、`deflate`表示指定格式
---@return any
function grpcio:decode(message_name, rawdata, compressed)
  assert(type(message_name) == 'string' and type(rawdata) == 'string', "Invalid GRPC `message_name` or `rawdata`")
  if sunpack(">BI4", rawdata) == 0x01 then
    if compressed == "deflate" then
      return pbdecode(message_name, uncompress(rawdata:sub(6)))
    end
    if compressed == "gzip" then
      return pbdecode(message_name, gzuncompress(rawdata:sub(6)))
    end
  end
  return pbdecode(message_name, rawdata:sub(6))
end

return grpcio