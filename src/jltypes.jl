jldata(::Type{PostgresType{:date}}, ptr::Ptr{UInt8}) = Date(bytestring(ptr), "yyyy-mm-dd")

jldata(::Type{PostgresType{:timestamp}}, ptr::Ptr{UInt8}) = begin
    v = bytestring(ptr)
    s = search(v, ".")

    if s.start > 0
        dt = v[1:s.start]
        ms = v[s.start+1:end]

        if length(ms) > 3
            ms = ms[1:3]
        end

        dt = dt * ms
    else
        dt = v
    end

    DateTime(dt, "yyyy-mm-dd HH:MM:SS.s")
end

jldata(::Type{PostgresType{:timestamptz}}, ptr::Ptr{UInt8}) = begin
    v = bytestring(ptr)
    s = search(v, ".")
    dt = v[1:s.start]
    ms = v[s.start+1:end]

    if length(ms) > 3
        ms = ms[1:3]
    end

    dt = dt * ms
    tz = "UTC" * v[end-2:end]

    TimeZones.ZonedDateTime(DateTime(dt, "yyyy-mm-dd HH:MM:SS.s"), TimeZones.FixedTimeZone(tz))
end

jldata(::Type{PostgresType{:bool}}, ptr::Ptr{UInt8}) = bytestring(ptr) != "f"

jldata(::Type{PostgresType{:int8}}, ptr::Ptr{UInt8}) = parse(Int64, bytestring(ptr))

jldata(::Type{PostgresType{:int4}}, ptr::Ptr{UInt8}) = parse(Int32, bytestring(ptr))

jldata(::Type{PostgresType{:int2}}, ptr::Ptr{UInt8}) = parse(Int16, bytestring(ptr))

jldata(::Type{PostgresType{:float8}}, ptr::Ptr{UInt8}) = parse(Float64, bytestring(ptr))

jldata(::Type{PostgresType{:float4}}, ptr::Ptr{UInt8}) = parse(Float32, bytestring(ptr))

function jldata(::Type{PostgresType{:numeric}}, ptr::Ptr{UInt8})
    s = bytestring(ptr)
    return parse(search(s, '.') == 0 ? BigInt : BigFloat, s)
end

jldata(::PGStringTypes, ptr::Ptr{UInt8}) = bytestring(ptr)

jldata(::Type{PostgresType{:bytea}}, ptr::Ptr{UInt8}) = bytestring(ptr) |> decode_bytea_hex

jldata(::Type{PostgresType{:unknown}}, ptr::Ptr{UInt8}) = bytestring(ptr)

jldata(::Type{PostgresType{:json}}, ptr::Ptr{UInt8}) = JSON.parse(bytestring(ptr))

jldata(::Type{PostgresType{:jsonb}}, ptr::Ptr{UInt8}) = JSON.parse(bytestring(ptr))

for (pt, t) in (
    (:array_int4, Int32),
    (:array_int8, Int64),
    (:array_float4, Float32),
    (:array_float8, Float64),
    )

    lt = Array{t}

    function jldata(::Type{PostgresType{pt}}, ptr::Ptr{UInt8})
        v = bytestring(ptr)[2:end-1]
        return isempty(v) ? t[] : t[(e == "NULL" ? nothing : convert(t, parse(t, e))) for e in split(v, ",")]
    end

    newpgdata(pt, lt, data -> "{" * join(data, ",") * "}")
end

for (pt, t) in (
    (:tuple_int4, Int32),
    (:tuple_int8, Int64),
    (:tuple_float4, Float32),
    (:tuple_float8, Float64)
    )

    lt = Tuple{Vararg{t}}

    function jldata(::Type{PostgresType{pt}}, ptr::Ptr{UInt8})
        v = bytestring(ptr)[2:end-1]
        return isempty(v) ? () : tuple(t[(e == "NULL" ? nothing : convert(t, parse(t, e))) for e in split(v, ",")]...)
    end

    newpgdata(pt, lt, data -> "(" * join(data, ",") * ")")
end

newpgdata(:null, Void, (data) -> "NULL")
newpgdata(:bool, Bool, (data) -> data ? "TRUE" : "FALSE")
newpgdata(:int8, Number, data -> string(convert(Int64, data)))
newpgdata(:int4, Number, data -> string(convert(Int32, data)))
newpgdata(:int2, Number, data -> string(convert(Int16, data)))
newpgdata(:float8, Number, data -> string(convert(Float64, data)))
newpgdata(:float4, Number, data -> string(convert(Float32, data)))
newpgdata(:numeric, Number, data -> string(data))
newpgdata(:date, Date, data -> string(data))
newpgdata(:timestamp, DateTime, data -> string(data))
newpgdata(:timestamptz, TimeZones.ZonedDateTime, data -> bytestring(string(data)))
newpgdata(:bytea, Vector{UInt8}, data -> bytestring("\\x", bytes2hex(data)))
newpgdata(:unknown, Any, v -> string(data))
newpgdata(:json, Dict{AbstractString,Any}, data -> bytestring(JSON.json(data)))
newpgdata(:jsonb, Dict{AbstractString,Any}, data -> JSON.json(data))
newpgdata(:jsonb, PgJSONb, data -> JSON.json(data.data))
newpgdata(PG_STRING_NAMES, ByteString, data -> data)
newpgdata(PG_STRING_NAMES, AbstractString, data -> data)