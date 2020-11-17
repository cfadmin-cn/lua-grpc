local print = print
local assert = assert

local fmt = string.format
local gsub = string.gsub
local concat = table.concat

local io_open = io.open

local json = require "json"

local lfs = require "lfs"
local mkdir = lfs.mkdir
local stat = lfs.attributes

local IO = { version = "0.1" }

local client_template = [[
local class = require "class"

local client = class("%s")

local type = type
local pairs = pairs
local assert = assert

function client:ctor(opt)
  assert(type(opt) == 'table', "Invalid rpc parms.")
	self.grpc = assert(opt.grpc, "rpc client need grpc ctx.")
	self.httpc = assert(opt.httpc, "rpc client need http2 ctx.")
	self.map = %s
end

function client:call(method, headers, body, timeout)
	local h = {
    ["te"] = "trailers",
    ["content-type"] = "application/grpc",
    ["grpc-accept-encoding"] = "gzip, identity"
  }
  local q = assert(type(method) == 'string' and self.map[method], "Can't find rpc method.")
  assert(type(body) == "table", "Invalid headers.")
  if type(headers) == "table" then
	  for k, v in pairs(headers) do
	  	h[k] = v
	  end
	end
	local response, errinfo = self.httpc:request(q.url, "POST", h, self.grpc:encode(q.req, body), timeout)
	if type(response) ~= 'table' or not response.body or response.body == '' then
		return nil, errinfo or "error response."
	end
	response.body = self.grpc:decode(q.resp, response.body)
	return response
end

return client
]]

local server_template = [[
local server = { name = "%s" }

%s
return server
]]

local function fmt_client(folder, filename, info)
	for method, id in pairs(info) do
		id.req = id.pkg .. id.req
		id.resp = id.pkg .. id.resp
		id.url = concat({"/", filename, "/", method})
	end
	return fmt(client_template, filename, fmt("require(\"%s.map\")['%s']", folder, filename))
end

local function fmt_server(folder, filename, info)
	local tab = {}
	for method, id in pairs(info) do
		tab[#tab+1] = fmt([[
function server.%s(context)
	return pbencode("%s", { })
end
]], method, id.pkg .. id.resp)
	end
	return fmt(server_template, filename, concat(tab, "\r\n"))
end

-- 自动生成代码文件(不会覆盖文件)
function IO.toFile(folder, filename, info)

	-- 开始检查并创建文件
	print("Start creating `" .. filename .. "` service：")

	-- 检查是否需要创建目录
	local client, server = concat({folder, "client"}, "/"), concat({folder, "server"}, "/")
	local cinfo, sinfo = stat(client), stat(server)
	if not cinfo or cinfo.mode ~= "directory" then
		mkdir(client)
	end
	if not server or server.mode ~= "directory" then
		mkdir(server)
	end

	-- 创建客户端文件
	local cfilename, sfilename = client .. "/".. filename .. ".lua", server .. "/".. filename .. ".lua"
	local cstat, sstat = stat(cfilename), stat(sfilename)
	if not cstat or cstat.mode ~= "file" then
		local f = assert(io_open(cfilename, "w"))
		f:write(fmt_client(folder, filename, info))
		f:flush()
		f:close()
		print("  Create client file: " .. cfilename)
	end
	if not sstat or sstat.mode ~= "file" then
		local f = assert(io_open(sfilename, "w"))
		f:write(fmt_server(folder, filename, info))
		f:flush()
		f:close()
		print("  Create server file: " .. sfilename)
	end

	return print("  OK.")
end

-- 去除单行注释与多行注释
function IO.toUncomment(proto)
  return gsub(gsub(proto, "%/%/[^%\r%\n]+", ""), "/%*[^%*%/]+*/", "")
end

-- 写入配置文件: fordel .. "/map.lua"
function IO.writeFile(filename, map)
  local f = assert(io.open(filename, "wb"), "Failed to create `map.lua` file.")
  f:write("return require('json').decode([[" .. json.encode(map) .."]])")
  f:flush()
  f:close()
end
return IO