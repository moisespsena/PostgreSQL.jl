for (pt, t) in (
    (:_int4, Int32),
    (:_int8, Int64),
    (:_float4, Float32),
    (:_float8, Float64),
    )

    pgserialize(::oidt(pt), v::rjl(pt, Vector{t}))= "{" * join(data, ",") * "}"
end


#for (pt, t) in (
#    (:tuple_int4, Int32),
#    (:tuple_int8, Int64),
#    (:tuple_float4, Float32),
#    (:tuple_float8, Float64)
#    )
#
#    pgserialize(::oid(pt), v::rjl(pt, t))= "(" * join(data, ",") * "}"
#end

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
pgserialize(::oidt(:json), v::rjl(:json, Dict{AbstractString,Any})) = bytestring(JSON.json(v))
pgserialize(::oidt(:jsonb), v::rjl(:jsonb, Dict{AbstractString,Any})) = JSON.json(v)
pgserialize(::oidt(:jsonb), v::rjl(:jsonb, JSONB)) = JSON.json(v.data)

for stype in PG_STRINGS
    pgserialize(::oidt(stype), v::rjl(oid(stype), String)) = v
end
