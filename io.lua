local print = print
local assert = assert

local fmt = string.format
local gsub = string.gsub
local concat = table.concat

local io_open = io.open

local lfs = require "lfs"
local mkdir = lfs.mkdir
local stat = lfs.attributes

local IO = { version = "0.1" }

local client_template = [[
local class = require "class"

local client = class("%s")

function client:ctor(opt)
	self.grpc = opt.grpc
	self.httpc = opt.httpc
	self.map = {%s}
end

function client:call(method, url, headers, body, timeout)
	local response, errinfo = self.httpc:request(url, headers, self.grpc:encode(self.map[method].req, body), timeout)
	if not response then
		return nil, errinfo
	end
	response.body = self.grpc:decode(self.map[method].resp, response.body)
	return response
end

return client
]]

local server_template = [[
local server = { name = "%s" }

%s
return server
]]

local function fmt_client(filename, info)
	local tab = {}
	-- var_dump(info)
	for method, id in pairs(info) do
		tab[#tab+1] = method .. " = " .. "{ " .. ( "req = '" .. id.pkg .. id.req .. "', ") .. ( "resp = '" .. id.pkg .. id.resp .. "'") .. " }"
	end
	return fmt(client_template, filename, concat(tab, ", "))
end

local function fmt_server(filename, info)
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
		f:write(fmt_client(filename, info))
		f:flush()
		f:close()
		print("  Create client file: " .. cfilename)
	end
	if not sstat or sstat.mode ~= "file" then
		local f = assert(io_open(sfilename, "w"))
		f:write(fmt_server(filename, info))
		f:flush()
		f:close()
		print("  Create server file: " .. sfilename)
	end

	return print("  OK.")
end

-- 去除注释
function IO.toUncomment(proto)
  -- proto = gsub(proto, "%/%/[^%\r%\n]+", "")   -- 去除单行注释
  -- proto = gsub(proto, "/%*[^%*%/]+*/", "")    -- 去除多行注释
  -- return proto
  -- return proto:gsub("%/%/[^%\r%\n]+", ""):gsub("/%*[^%*%/]+*/", "")
  return gsub(gsub(proto, "%/%/[^%\r%\n]+", ""), "/%*[^%*%/]+*/", "")
end

return IO