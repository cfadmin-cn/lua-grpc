local aio = require "aio"

local httpd2 = require "lua-http2.httpd"

local protoc = require "protobuf.protoc"

local pb = require "protobuf"
local pbencode = pb.encode
local pbdecode = pb.decode

local lz = require"lz"
local uncompress = lz.uncompress
local gzuncompress = lz.gzuncompress

local LOG = require "logging"

local type = type
local pcall = pcall
local assert = assert
local fmt = string.format
local gsub = string.gsub
local match = string.match
local splite = string.gmatch
local strpack = string.pack
local strunpack = string.unpack

local service_regex = "service[ ]+([^ %{%}]+)[ ]*{(.-)}"
local service_mathod_regex = "rpc[ ]*([^ ]+)[ ]*%(([^%)%(]+)%)[ ]*returns[ ]*%(([^%)%(]+)%)[ ]*[;]?"

local class = require "class"

local server = class("grpc-server")

local grpc_code = {
  GRPC_STATUS_DO_NOT_USE = -1,
  GRPC_STATUS_OK = 0,
  GRPC_STATUS_CANCELLED = 1,
  GRPC_STATUS_UNKNOWN = 2,
  GRPC_STATUS_INVALID_ARGUMENT = 3,
  GRPC_STATUS_DEADLINE_EXCEEDED = 4,
  GRPC_STATUS_NOT_FOUND = 5,
  GRPC_STATUS_ALREADY_EXISTS = 6,
  GRPC_STATUS_PERMISSION_DENIED = 7,
  GRPC_STATUS_RESOURCE_EXHAUSTED = 8,
  GRPC_STATUS_FAILED_PRECONDITION = 9,
  GRPC_STATUS_ABORTED = 10,
  GRPC_STATUS_OUT_OF_RANGE = 11,
  GRPC_STATUS_UNIMPLEMENTED = 12,
  GRPC_STATUS_INTERNAL = 13,
  GRPC_STATUS_UNAVAILABLE = 14,
  GRPC_STATUS_DATA_LOSS = 15,
  GRPC_STATUS_UNAUTHENTICATED = 16,
}

function server:ctor()
  self.h2 = httpd2:new()
  self.protoc = protoc:new()
  self.services = {}
  self.log = LOG:new { dumped = true, path = "grpc-client"}
end

---comment 从字符串内容加载protobuf协议
---@param proto string @protobuf内容
---@return table @grpc client
function server:load(proto)
  proto = gsub(gsub(proto, "%/%/[^%\r%\n]+", ""), "/%*.+%*/", "")
  if self.protoc:load(proto) and self.services then
    local pkg = match(proto, "package ([^ ;]+)[ ;]-") or ""
    pkg = pkg ~= "" and (pkg .. ".") or pkg
    for service, service_list in splite(proto, service_regex) do
      self.services[service] = assert(not self.services[service] and {}, '[GRPC-CLIENT ERROR]: service[`' .. service .. "`] is repeatedly defined")
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
---@return table @grpc client
function server:loadfile(filename)
  -- 尝试读取文件
  local f = assert(io.open(filename, "rb"))
  self:load(f:read "*a")
  f:close()
  return self
end

---comment 从给定的文件路径中加载`protobuf`协议文件
---@param dir string @`pb`文件存储目录
---@return table @grpc client
function server:loaddir(dir)
  for _, fname in ipairs(aio.dir(dir)) do
    if (fname ~= '.' and fname ~= '..') and (fname:match(".+%.pb$") or fname:match(".+%.proto$") or fname:match(".+%.pb2$") or fname:match(".+%.pb3$")) then
      self:loadfile(fname)
    end
  end
  return self
end

---comment 注册`grpc`服务
---@param sname     string      @服务端名称
---@param mname     string      @方法名称
---@param callback  function    @请求回调
function server:service(sname, mname, callback)
  local obj = assert(self.services[sname] and self.services[sname][mname], "[GRPC-SERVER ERROR]: Invalid grpc service or method.")
  return self.h2:route(fmt("/%s/%s", sname, mname), function(req, resp)
    if type(req.body) ~= 'string' or ( req.headers['content-type'] ~= 'application/grpc' and req.headers['content-type'] ~= 'application/grpc-proto') then
      resp.code, resp.headers['grpc-status'], resp.headers['grpc-message'] = 200, grpc_code["GRPC_STATUS_DATA_LOSS"], "[GRPC-SERVER ERROR]: Invalid request body."
      return
    end
    -- print(obj.pkg .. obj.req)
    -- 将请求体解码为`table`
    local pbobject, err = self:decode(obj.pkg .. obj.req, req.body)
    if not pbobject then
      resp.code, resp.headers['grpc-status'], resp.headers['grpc-message'] = 200, grpc_code["GRPC_STATUS_DATA_LOSS"], "[GRPC-SERVER ERROR]: " .. (err or "Invalid protobuf.")
      return
    end
    -- 请求体作为请求参数回调内容
    local ok, info = pcall(callback, req.headers, pbobject)
    if not ok then
      resp.code, resp.headers['grpc-status'], resp.headers['grpc-message'] = 200, grpc_code['GRPC_STATUS_INTERNAL'], "[GRPC-SERVER ERROR]: " .. info
      return
    end
    -- 如果回应的数据`table`, 那么久由框架来选择编码.
    if type(info) ~= 'string' then
      if type(info) ~= 'table' then
        resp.code, resp.headers['grpc-status'], resp.headers['grpc-message'] = 200, grpc_code['GRPC_STATUS_INTERNAL'], "[GRPC-SERVER ERROR]: Invalid grpc server response in handle."
        return
      end
      -- print(obj.pkg .. obj.resp)
      info, err = self:encode(obj.pkg .. obj.resp, info)
      if not info then
        resp.code, resp.headers['grpc-status'], resp.headers['grpc-message'] = 200, grpc_code['GRPC_STATUS_INTERNAL'], "[GRPC-SERVER ERROR]: " .. info
        return
      end
    end
    resp.code, resp.headers['grpc-status'], resp.headers['grpc-accept-encoding'], resp.headers['accept-encoding'], resp.headers['content-type'], resp.body = 200, grpc_code['GRPC_STATUS_OK'], "identity,gzip", "identity,gzip", "application/grpc", info
    -- var_dump(resp)
  end)
end

---comment `GRPC`的序列化方法
---@param message_name string @package与message name
---@param message_table table @待序列化的table
---@return string @序列化成功将返回table
function server:encode(message_name, message_table)
  assert(type(message_name) == 'string' and type(message_table) == 'table', "[GRPC-CLIENT ERROR]: Invalid GRPC `message_name` or `message_table`")
  local pbmsg = pbencode(message_name, message_table)
  return strpack(">BI4", 0x00, #pbmsg) .. pbmsg
end

---comment `GRPC`的反序列化方法
---@param message_name string @package与message name
---@param rawdata string @待反序列化的string
---@param compressed string | nil @`nil`表示不需要未压缩, `gzip`、`deflate`表示指定格式
---@return any
function server:decode(message_name, rawdata, compressed)
  assert(type(message_name) == 'string' and type(rawdata) == 'string', "[GRPC-CLIENT ERROR]: Invalid GRPC `message_name` or `rawdata`")
  if strunpack(">BI4", rawdata) == 0x01 then
    if compressed == "deflate" then
      return pbdecode(message_name, uncompress(rawdata:sub(6)))
    end
    if compressed == "gzip" then
      return pbdecode(message_name, gzuncompress(rawdata:sub(6)))
    end
  end
  return pbdecode(message_name, rawdata:sub(6))
end

---comment 监听`grpc`端口
---@param ip string
---@param port integer
---@param backlog integer
function server:listen(ip, port, backlog)
  return self.h2:listen(ip, port, backlog)
end

---comment `grpc server`开始运行
function server:run()
  return self.h2:run()
end

return server