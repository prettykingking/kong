local fmt = string.format


local Strategies = {}


Strategies.STRATEGIES   = {
  ["postgres"]  = true,
  ["cassandra"] = true,
}


function Strategies.new_connector(kong_config, database)
  database = database or kong_config.database

  if not Strategies.STRATEGIES[database] then
    error("unknown strategy: " .. database, 2)
  end

  -- strategy-specific connector with :connect() :setkeepalive() :query() ...
  local Connector = require(fmt("kong.db.strategies.%s.connector", database))

  local connector, err = Connector.new(kong_config)
  if not connector then
    return nil, nil, err
  end

  do
    local base_connector = require "kong.db.strategies.connector"
    local mt = getmetatable(connector)
    setmetatable(mt, { __index = base_connector })
  end

  return connector
end


function Strategies.new_strategy(connector, schema, errors)
  local database = connector.name

  if not Strategies.STRATEGIES[database] then
    error("unknown strategy: " .. database, 2)
  end

  -- strategy-specific automated CRUD query builder with :insert() :select()
  local Strategy = require("kong.db.strategies." .. database)

  local strategy, err = Strategy.new(connector, schema, errors)
  if not strategy then
    return nil, nil, err
  end

  if Strategy.CUSTOM_STRATEGIES then
    local custom_strategy = Strategy.CUSTOM_STRATEGIES[schema.name]

    if custom_strategy then
      local parent_mt = getmetatable(strategy)
      local mt = {
        __index = function(t, k)
          -- explicit parent
          if k == "super" then
            return parent_mt
          end

          -- override
          local f = custom_strategy[k]
          if f then
            return f
          end

          -- parent fallback
          return parent_mt[k]
        end
      }

      setmetatable(strategy, mt)
    end
  end

  return strategy
end


return Strategies
