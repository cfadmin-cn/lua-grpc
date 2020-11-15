local class = require "class"

local h2 = require "http2.client"

local client = class("grpc-client")

function client:ctor(opt)
  self.version = "0.1"
	self.domain = opt.domain
  self.max = opt.max or 1
end

function client:connect()
	-- body
end

return client