pgserialize(::oidt(:_int4), v::rjl(:_int4, Vector{Int32})) = "{" * join(map(string, v), ",") * "}"
pgserialize(::oidt(:_int4), v::rjl(:_int4, Tuple{Vararg{Int32}}))= "(" * join(v, ",") * ")"
pgserialize(::oidt(:_int8), v::rjl(:_int8, Vector{Int64})) = "{" * join(map(string, v), ",") * "}"
pgserialize(::oidt(:_int8), v::rjl(:_int8, Tuple{Vararg{Int64}}))= "(" * join(v, ",") * ")"
pgserialize(::oidt(:_float4), v::rjl(:_float4, Vector{Float32})) = "{" * join(map(string, v), ",") * "}"
pgserialize(::oidt(:_float4), v::rjl(:_float4, Tuple{Vararg{Float32}}))= "(" * join(v, ",") * ")"
pgserialize(::oidt(:_float8), v::rjl(:_float8, Vector{Float64})) = "{" * join(map(string, v), ",") * "}"
pgserialize(::oidt(:_float8), v::rjl(:_float8, Tuple{Vararg{Float64}}))= "(" * join(v, ",") * ")"
pgserialize(::oidt(:null), v::rjl(:null, Void)) = "NULL"
pgserialize(::oidt(:bool), v::rjl(:bool, Bool)) = v ? "TRUE" : "FALSE"
pgserialize(::oidt(:int8), v::rjl(:int8, Int64)) = string(v)
pgserialize(::oidt(:int4), v::rjl(:int4, Int32)) = string(v)
pgserialize(::oidt(:int2), v::rjl(:int2, Number)) = string(convert(Int16, v))
pgserialize(::oidt(:float8), v::rjl(:float8, BigFloat)) = string(v)
pgserialize(::oidt(:float8), v::rjl(:float8, Float64)) = string(v)
pgserialize(::oidt(:float4), v::rjl(:float4, Number)) = string(convert(Float32, v))
pgserialize(::oidt(:float4), v::rjl(:float4, Float32)) = string(v)
pgserialize(::oidt(:numeric), v::rjl(:numeric, Number)) = string(v)
pgserialize(::oidt(:numeric), v::rjl(:numeric, BigInt)) = string(v)
pgserialize(::oidt(:date), v::rjl(:date, Date)) = string(v)
pgserialize(::oidt(:timestamp), v::rjl(:timestamp, DateTime)) = string(v)
pgserialize(::oidt(:timestamptz), v::rjl(:timestamptz, TimeZones.ZonedDateTime)) = bytestring(string(v))
pgserialize(::oidt(:bytea), v::rjl(:bytea, Vector{UInt8})) = string("\\x", bytes2hex(v))
pgserialize(::oidt(:unknown), v::rjl(:unknown, Any)) = string(v)
pgserialize(::oidt(:json), v::rjl(:json, Dict{String,Any})) = JSON.json(v)
pgserialize(::oidt(:jsonb), v::rjl(:jsonb, Dict{String,Any})) = JSON.json(v)
pgserialize(::oidt(:jsonb), v::rjl(:jsonb, JSONB)) = isa(v.data, String) ? v.data : JSON.json(v.data)
pgserialize(::oidt(:_varchar), v::rjl(:_varchar, Vector)) =
    string("ARRAY['" * join(Any[replace(v, "'", "''") for v in data], "', '") * "']")

for stype in PG_STRINGS
    pgserialize(::oidt(stype), v::rjl(oid(stype), String)) = v
end
