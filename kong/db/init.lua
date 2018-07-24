local DAO          = require "kong.db.dao"
local Entity       = require "kong.db.schema.entity"
local Errors       = require "kong.db.errors"
local Strategies   = require "kong.db.strategies"
local MetaSchema   = require "kong.db.schema.metaschema"


local fmt          = string.format
local type         = type
local pairs        = pairs
local error        = error
local ipairs       = ipairs
local rawget       = rawget
local setmetatable = setmetatable


-- maybe a temporary constant table -- could be move closer
-- to schemas and entities since schemas will also be used
-- independently from the DB module (Admin API for GUI)
local CORE_ENTITIES = {
  "consumers",
  "routes",
  "services",
  "certificates",
  "snis",
}


local DB = {}
DB.__index = function(self, k)
  return DB[k] or rawget(self, "daos")[k]
end


function DB.new(kong_config, strategy)
  if not kong_config then
    error("missing kong_config", 2)
  end

  if strategy ~= nil and type(strategy) ~= "string" then
    error("strategy must be a string", 2)
  end

  -- load errors

  local errors = Errors.new(strategy or kong_config.database)

  local connector, err = Strategies.new_connector(kong_config, strategy)
  if err then
    return nil, err
  end

  local daos = {}
  local strategies = {}

  local self = setmetatable({
    daos = daos,       -- each of those has the connector singleton
    connector = connector,
    strategies = strategies,
    errors = errors,
  }, DB)

  for _, entity_name in ipairs(CORE_ENTITIES) do
    local entity_table = require("kong.db.schema.entities." .. entity_name)
    local ok, err = self:load_entity(entity_table)
    if not ok then
      return nil, err
    end
  end

  -- we are 200 OK

  return self
end


function DB:load_entity(entity_definition)
  if type(entity_definition) ~= "table" then
    error("entity_definition must be a table", 2)
  end

  -- validate entity definition via metaschema
  local ok, err_t = MetaSchema:validate(entity_definition)
  if not ok then
    return nil, fmt("schema of entity '%s' is invalid: %s",
                    entity_definition.name,
                    tostring(self.errors:schema_violation(err_t)))
  end

  local schema = Entity.new(entity_definition)

  local strat = Strategies.new_strategy(self.connector, schema, self.errors)
  if not strat then
    return nil, fmt("no strategy found for schema '%s'", schema.name)
  end

  self.daos[schema.name] = DAO.new(self, schema, strat, self.errors)

  return true
end


function DB:init_connector()
  -- I/O with the DB connector singleton
  -- Implementation up to the strategy's connector. A place for:
  --   - connection check
  --   - cluster retrievel (cassandra)
  --   - prepare statements
  --   - nop (default)

  return self.connector:init()
end


function DB:connect()
  return self.connector:connect()
end


function DB:setkeepalive()
  return self.connector:setkeepalive()
end


function DB:reset()
  return self.connector:reset()
end


function DB:truncate()
  return self.connector:truncate()
end


function DB:set_events_handler(events)
  for _, dao in pairs(self.daos) do
    dao.events = events
  end
end


return DB
