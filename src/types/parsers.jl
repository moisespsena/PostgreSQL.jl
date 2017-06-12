for (pt, t) in (
    (:_int4, Int32),
    (:_int8, Int64),
    (:_float4, Float32),
    (:_float8, Float64),
    )

    function pgparse(::oidt(pt), ptr::Ptr{UInt8})
        v = unsafe_string(ptr)[2:end-1]
        return isempty(v) ? t[] : t[(e == "NULL" ? nothing :
            convert(t, parse($t, e))) for e in split(v, ",")]
    end
end

pgparse(::oidt(:date), ptr::Ptr{UInt8}) = Date(unsafe_string(ptr), "yyyy-mm-dd")

pgparse(::oidt(:timestamp), ptr::Ptr{UInt8}) = begin
    v = unsafe_string(ptr)
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

pgparse(::oidt(:timestamptz), ptr::Ptr{UInt8}) = begin
    v = unsafe_string(ptr)
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

pgparse(::oidt(:bool), ptr::Ptr{UInt8}) = unsafe_string(ptr) != "f"
pgparse(::oidt(:int8), ptr::Ptr{UInt8}) = parse(Int64, unsafe_string(ptr))
pgparse(::oidt(:int4), ptr::Ptr{UInt8}) = parse(Int32, unsafe_string(ptr))
pgparse(::oidt(:int2), ptr::Ptr{UInt8}) = parse(Int16, unsafe_string(ptr))
pgparse(::oidt(:float8), ptr::Ptr{UInt8}) = parse(Float64, unsafe_string(ptr))
pgparse(::oidt(:float4), ptr::Ptr{UInt8}) = parse(Float32, unsafe_string(ptr))
function pgparse(::oidt(:numeric), ptr::Ptr{UInt8})
    s = unsafe_string(ptr)
    return parse(search(s, '.') == 0 ? BigInt : BigFloat, s)
end
pgparse(::PGStrings, ptr::Ptr{UInt8}) = unsafe_string(ptr)
pgparse(::oidt(:bytea), ptr::Ptr{UInt8}) = unsafe_string(ptr) |> decode_bytea_hex
pgparse(::oidt(:unknown), ptr::Ptr{UInt8}) = unsafe_string(ptr)
pgparse(::oidt(:json), ptr::Ptr{UInt8}) = JSON.parse(unsafe_string(ptr))
pgparse(::oidt(:jsonb), ptr::Ptr{UInt8}) = JSON.parse(unsafe_string(ptr))

for stype in PG_STRINGS
    pgparse(::oidt(stype), ptr::Ptr{UInt8}) = unsafe_string(ptr)
end
