import DataArrays: NAtype
import JSON
import Compat: Libc, unsafe_convert, parse, @compat
import TimeZones

include("types/base.jl")

Base.convert{T}(::Type{Oid}, ::Type{OID{T}}) = convert(Oid, T)

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
        types = DataType[OID{Int(PQftype(result, col))}
        for col in 0:(nfields-1)]
    else
        types = DataType[]
    end

    println(types)

    return PostgresResultHandle(result, types, PQntuples(result), nfields)
end

type PostgresStatementHandle <: DBI.StatementHandle
    db::PostgresDatabaseHandle
    stmt::AbstractString
    executed::Int
    paramtypes::Array{DataType}
    finished::Bool
    result::PostgresResultHandle

    function PostgresStatementHandle(db::PostgresDatabaseHandle,
      stmt::AbstractString, executed=0, paramtypes::Array{DataType}=DataType[])
        new(db, stmt, executed, paramtypes, false)
    end
end

function Base.copy(rh::PostgresResultHandle)
    PostgresResultHandle(PQcopyResult(result, PG_COPYRES_ATTRS |
      PG_COPYRES_TUPLES | PG_COPYRES_NOTICEHOOKS | PG_COPYRES_EVENTS),
      copy(rh.types), rh.ntuples, rh.nfields)
end

type PostgresException <: Exception
    status
    msg
end

PostgresException(db::PostgresDatabaseHandle) =
    PostgresQueryException(PQstatus(db.ptr), bytestring(PQerrorMessage(db.ptr)))

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

fields(r::PostgresResultHandle) =
    tuple(AbstractString[bytestring(PostgreSQL.PQfname(r.ptr, i-1))
        for i = 1:length(r.types)]...)

function escapeliteral(db::PostgresDatabaseHandle, value::AbstractString)
    strptr = PQescapeLiteral(db.ptr, value, sizeof(value))
    str = bytestring(strptr)
    PQfreemem(strptr)
    return str
end

include("types/types.jl")
