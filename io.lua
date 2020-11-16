local print = print
local assert = assert
local fmt = string.format

local concat = table.concat

local io_open = io.open

local lfs = require "lfs"
local mkdir = lfs.mkdir
local stat = lfs.attributes

local IO = { version = "0.1" }

local client_template = [[
local class = require "class"

local client = class("%s")

%s

return client
]]
local server_template = [[
local class = require "class"

local server = class("%s")

return server
]]

local function fmt_client(filename, info)
	return fmt(client_template, filename)
end

local function fmt_server(filename, info)
	return fmt(server_template, filename)
end

-- 自动生成代码文件, 永远不会覆盖文件
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

return IO