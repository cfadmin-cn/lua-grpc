local pb = require "protobuf"
local pbencode = pb.encode
local pbdecode = pb.decode
local pbloadfile = pb.loadfile

local lz = require"lz"
local gzcompress = lz.gzcompress
local gzuncompress = lz.gzuncompress

local ipairs = ipairs
local spack = string.pack
local sunpack = string.unpack

local class = require "class"

local grpc = class("class")

function grpc:ctor(opt)
  self.version = "0.1"
  self.compressed = false
  if type(opt) == 'table' then
    self.compressed = opt.compressed and 0x01
  end 
end

function grpc:loadfile(filename)
  return pbloadfile(filename) and self
end

function grpc:loadfiles(file_list)
  for _, filename in ipairs(file_list) do
    self:loadfile(filename)
  end
end

function grpc:encode(message_name, message_table)
  local pbmsg = pbencode(message_name, message_table)
  if self.compressed then
    pbmsg = gzcompress(pbmsg)
  end
  return spack(">BI4", self.compressed or 0x00, #pbmsg) .. pbmsg
end

function grpc:decode(message_name, rawdata)
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