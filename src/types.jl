import DataArrays: NAtype
import JSON
import Compat: Libc, unsafe_convert, parse, @compat
import TimeZones

type NULL end

abstract AbstractPostgresType
type PostgresType{Name} <: AbstractPostgresType end

abstract AbstractOID
type OID{N} <: AbstractOID end

oid{T<:AbstractPostgresType}(t::Type{T}) = convert(OID, t)
pgtype(t::Type) = convert(PostgresType, t)

Base.convert{T}(::Type{Oid}, ::Type{OID{T}}) = convert(Oid, T)

function newpgtype(pgtypename::Symbol, oid::Signed, jltypes::Tuple{Vararg{Type}})
    Base.convert(::Type{OID{oid}}, ::Type{PostgresType{pgtypename}}) = OID{oid}
    # 20170608
    #Base.convert(::Type{PostgresType{pgtypename}}, ::Type{OID{oid}}) = PostgresType{pgtypename}
    Base.convert(::Type{PostgresType}, ::Type{OID{oid}}) = PostgresType{pgtypename}

    for jt=jltypes
        @eval Base.convert{S<:$jt}(::Type{PostgresType}, ::Type{S}) = PostgresType{pgtypename}
    end
end

function Base.convert(a::Type{PostgreSQL.PostgresType}, b::Type{Union})
    error("$(typeof(a)) -> $a, $(typeof(b)) -> $b")
end

module PGData
    function pgdata() end
    function pgdataraw() end
end
import .PGData

const pgdata = PGData.pgdata
const pgdataraw = PGData.pgdataraw

if VERSION >= v"0.5"
    const bytestring = unsafe_string
end

function newpgdata(tp::Symbol, tpd, fn)
    PGData.pgdataraw(::Type{PostgresType{tp}}, data::tpd) = fn(data)
    function PGData.pgdata(ct::Type{PostgresType{tp}}, ptr::Ptr{UInt8}, data::tpd)
        ptr = storestring!(ptr, bytestring(PGData.pgdataraw(ct, data)))
    end
end

function newpgdata(tps::Tuple{Vararg{Symbol}}, tpd, fn)
    for tp in tps
        newpgdata(tp, tpd, fn)
    end
end

type PgJSONb
    data::Union{Dict,Array}
end

export PgJSONb

newpgtype(:null, 0, (Void,))
newpgtype(:bool, 16, (Bool,))
newpgtype(:bytea, 17, (Vector{UInt8},))
newpgtype(:int8, 20, (Int64,))
newpgtype(:int4, 23, (Int32,))
newpgtype(:int2, 21, (Int16,))
newpgtype(:float4, 700, (Float32,))
newpgtype(:float8, 701, (Float64,))
newpgtype(:float4, 1021, (Float32,))
newpgtype(:float8, 1022, (Float64,))
newpgtype(:bpchar, 1042, ())
newpgtype(:varchar, 1043, (ASCIIString,UTF8String))
newpgtype(:text, 25, ())
newpgtype(:numeric, 1700, (BigInt,BigFloat))
newpgtype(:date, 1082, (Date,))
newpgtype(:timestamp, 1114, (DateTime,))
newpgtype(:timestamptz, 1184, (TimeZones.ZonedDateTime,))
#newpgtype(:unknown, 705, (Union,NAtype,Void, AbstractString))
newpgtype(:json, 114, (Dict{AbstractString,Any},))
newpgtype(:jsonb, 3802, (Dict{AbstractString,Any},PgJSONb))
newpgtype(:array_int4, 1007, (Array{Int32},))
newpgtype(:array_int8, 1016, (Array{Int64},))
#newpgtype(:tuple, 2249, (Tuple{Vararg{Any}},))
newpgtype(:tuple_int8, 2249, (Tuple{Int64,Int64},))
newpgtype(:array_float4, 1021, (Array{Float32},))
newpgtype(:array_float8, 1022, (Array{Float64},))

const PG_STRING_NAMES = (:bpchar, :varchar, :text, :date)
typealias PGStringTypes Union{Type{PostgresType{:bpchar}},
                              Type{PostgresType{:varchar}},
                              Type{PostgresType{:text}},
                              Type{PostgresType{:date}}}

function storestring!(ptr::Ptr{UInt8}, str::AbstractString)
    ptr = convert(Ptr{UInt8}, Libc.realloc(ptr, sizeof(str)+1))
    unsafe_copy!(ptr, unsafe_convert(Ptr{UInt8}, str), sizeof(str)+1)
    return ptr
end

# In text mode, pq returns bytea as a text string "\xAABBCCDD...", for instance
# UInt8[0x01, 0x23, 0x45] would come in as "\x012345"
function decode_bytea_hex(s::AbstractString)
    if length(s) < 2 || s[1] != '\\' || s[2] != 'x'
        error("Malformed bytea string: $s")
    end
    return hex2bytes(s[3:end])
end

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

#newpgdata(:tuple, Tuple{Vararg{Any}}, function(data)
#    ptr = storestring!(ptr, bytestring("(" * join(data, ",") * ")"))
#end

#newpgdata(:tuple_int8, Tuple{Vararg{Int64}}, function(data)
#    ptr = storestring!(ptr, bytestring("(" * join(data, ",") * ")"))
#end


# dbi
abstract Postgres <: DBI.DatabaseSystem

type PostgresDatabaseHandle <: DBI.DatabaseHandle
    ptr::Ptr{PGconn}
    status::ConnStatusType
    closed::Bool

    function PostgresDatabaseHandle(ptr::Ptr{PGconn}, status::ConnStatusType)
        new(ptr, status, false)
    end
end

type PostgresResultHandle
    ptr::Ptr{PGresult}
    types::Vector{DataType}
    nrows::Integer
    ncols::Integer
    state::Int

    PostgresResultHandle(ptr::Ptr{PGresult}, types::Vector{DataType},
        nrows::Integer, ncols::Integer) = new(ptr, types, nrows, ncols, 0)
end

function PostgresResultHandle(result::Ptr{PGresult})
    status = PQresultStatus(result)
    nfields = PQnfields(result)

    if status == PGRES_TUPLES_OK || status == PGRES_SINGLE_TUPLE
        oids = @compat [OID{Int(PQftype(result, col))} for col in 0:(nfields-1)]
        types = DataType[convert(PostgresType, x) for x in oids]
    else
        types = DataType[]
    end
    return PostgresResultHandle(result, types, PQntuples(result), nfields)
end

type PostgresStatementHandle <: DBI.StatementHandle
    db::PostgresDatabaseHandle
    stmt::AbstractString
    executed::Int
    paramtypes::Array{DataType}
    finished::Bool
    result::PostgresResultHandle

    function PostgresStatementHandle(db::PostgresDatabaseHandle, stmt::AbstractString, executed=0, paramtypes::Array{DataType}=DataType[])
        new(db, stmt, executed, paramtypes, false)
    end
end

function Base.copy(rh::PostgresResultHandle)
    PostgresResultHandle(PQcopyResult(result, PG_COPYRES_ATTRS | PG_COPYRES_TUPLES |
        PG_COPYRES_NOTICEHOOKS | PG_COPYRES_EVENTS), copy(rh.types), rh.ntuples, rh.nfields)
end

type PostgresException <: Exception
    status
    msg
end

PostgresException(db::PostgresDatabaseHandle) = PostgresQueryException(PQstatus(db.ptr), bytestring(PQerrorMessage(db.ptr)))

type PostgresQueryException <: Exception
    status
    status_text
    msg
end

function PostgresQueryException(result::Ptr{PGresult})
    status = PQresultStatus(result)
    PostgresQueryException(status, bytestring(PQresStatus(status)),
        bytestring(PQresultErrorMessage(result)))
end

fields(r::PostgresResultHandle) = tuple(AbstractString[bytestring(PostgreSQL.PQfname(r.ptr, i-1))
    for i = 1:length(r.types)]...)
