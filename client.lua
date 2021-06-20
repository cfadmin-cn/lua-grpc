local cf = require "cf"

local aio = require "aio"

local LOG = require "logging"

local httpc2 = require "lua-http2.httpc"

local protoc = require "protobuf.protoc"

local pb = require "protobuf"
local pbencode = pb.encode
local pbdecode = pb.decode

local lz = require"lz"
local uncompress = lz.uncompress
local gzuncompress = lz.gzuncompress

local type = type
local assert = assert
local toint = math.tointeger
local lower = string.lower
local fmt = string.format
local gsub = string.gsub
local match = string.match
local splite = string.gmatch
local strpack = string.pack
local strunpack = string.unpack

local tremove = table.remove
local tinsert = table.insert

local service_regex = "service[ ]+([^ %{%}]+)[ ]*{(.-)}"
local service_mathod_regex = "rpc[ ]*([^ ]+)[ ]*%(([^%)%(]+)%)[ ]*returns[ ]*%(([^%)%(]+)%)[ ]*[;]?"

-- 创建`h2-session`
local function new_session(self)
  while true do
    local httpc = httpc2:new { domain = self.domain }
    httpc:no_alpn()
    if httpc:connect() then
      return httpc
    end
    httpc:close()
    cf.sleep(1) -- 休息1秒后重试
    self.log:ERROR("[GRPC-CLIENT ERROR]: The GRPC Client will reconnect in 1 seconds.")
  end
end


local class = require "class"

local client = class("grpc-client")

function client:ctor(opt)
  assert(type(opt) == 'table', "[GRPC-CLIENT ERROR]: client need domain.")
  self.protoc = protoc:new()
  self.log = LOG:new { dumped = true, path = "grpc-client"}
  self.max = opt.max or 10
  self.psize = opt.max or 10
  self.pool = {}
  self.cos = {}
  self.services = {}
  self.domain = opt.domain
end

---comment 从字符串内容加载protobuf协议
---@param proto string @protobuf内容
---@return table @grpc client
function client:load(proto)
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
function client:loadfile(filename)
  -- 尝试读取文件
  local f = assert(io.open(filename, "rb"))
  self:load(f:read "*a")
  f:close()
  return self
end

---comment 从给定的文件路径中加载protobuf协议文件
---@param dir string @`pb`文件存储目录
---@return table @grpc client
function client:loaddir(dir)
  for _, fname in ipairs(aio.dir(dir)) do
    if (fname ~= '.' and fname ~= '..') and (fname:match(".+%.pb$") or fname:match(".+%.proto$") or fname:match(".+%.pb2$") or fname:match(".+%.pb3$")) then
      self:loadfile(fname)
    end
  end
  return self
end

---comment 发起远程调用
---@param sname string @服务名称
---@param mname string @方法名称
---@param body table @请求内容
---@param timeout number @重试时间
---@return table<any, any> | nil, string
function client:call(sname, mname, body, timeout)
  if self.psize > 0 then
    self.psize = self.psize - 1
    tinsert(self.pool, new_session(self))
  end
  local obj = assert(self.services[sname] and self.services[sname][mname], "[GRPC-CLIENT ERROR]: Invalid grpc service or method.")
  local session = tremove(self.pool)
  if not session then
    local co = cf.self()
    tinsert(self.cos, co)
    session = cf.wait()
  end
  -- 发送GRPC请求
  local response = session:request(fmt("/%s/%s", sname, mname), "POST", { ["te"] = "trailers", ["content-type"] = "application/grpc", ["grpc-accept-encoding"] = "gzip,identity" }, self:encode(obj.pkg .. obj.req, body), timeout)
  -- 如果网络有问题则断开连接
  if type(response) ~= 'table' then
    session:close()
    self.psize = self.psize + 1
    return self:call(sname, mname, body, timeout)
  end
  local co = tremove(self.cos)
  if not co then
    tinsert(self.pool, session)
  else
    cf.wakeup(co, session)
  end
  -- 服务器发生错误的时候.
  if toint(response["headers"]["grpc-status"]) ~= 0 then
    return false, response["headers"]["grpc-message"] or "[GRPC-CLIENT ERROR]: Unknown grpc error."
  end
  -- 如果不是GRPC响应类型
  local content_type = lower(response["headers"]["content-type"] or "")
  if content_type ~= "application/grpc" and content_type ~= "application/grpc+proto" then
    return false, "[GRPC-CLIENT ERROR]: Invalid grpc server content-type : " .. (response["headers"]["content-type"] or "Unknown.")
  end
  return self:decode(obj.pkg .. obj.resp, response.body, response["headers"]["grpc-encoding"])
end

---comment `GRPC`的序列化方法
---@param message_name string @package与message name
---@param message_table table @待序列化的table
---@return string @序列化成功将返回table
function client:encode(message_name, message_table)
  assert(type(message_name) == 'string' and type(message_table) == 'table', "[GRPC-CLIENT ERROR]: Invalid GRPC `message_name` or `message_table`")
  local pbmsg = pbencode(message_name, message_table)
  return strpack(">BI4", 0x00, #pbmsg) .. pbmsg
end

---comment `GRPC`的反序列化方法
---@param message_name string @package与message name
---@param rawdata string @待反序列化的string
---@param compressed string | nil @`nil`表示不需要未压缩, `gzip`、`deflate`表示指定格式
---@return any
function client:decode(message_name, rawdata, compressed)
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

return client