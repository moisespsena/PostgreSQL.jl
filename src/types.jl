#import DataArrays: NAtype
import JSON
import Compat: Libc, unsafe_convert, parse, @compat
import TimeZones

include("types/base.jl")

Base.convert{T}(::Type{Oid}, ::Type{OID{T}}) = convert(Oid, T)

# dbi
abstract Postgres #<: DBI.DatabaseSystem

type PostgresDatabaseHandle #<: DBI.DatabaseHandle
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

    return PostgresResultHandle(result, types, PQntuples(result), nfields)
end

type PostgresStatementHandle #<: DBI.StatementHandle
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

escapeliteral{T<:Number}(db::PostgresDatabaseHandle, value::T) = value

include("types/types.jl")


function Base.connect(::Type{Postgres},
                      host::AbstractString="",
                      user::AbstractString="",
                      passwd::AbstractString="",
                      db::AbstractString="",
                      port::AbstractString="")
    if contains(host, "://")
        # with dsn
        conn = PQconnectdb(host)
        status = PQstatus(conn)
        if status != CONNECTION_OK
            errmsg = bytestring(PQerrorMessage(conn))
            PQfinish(conn)
            error(errmsg)
        end
        conn = PostgresDatabaseHandle(conn, status)
        #finalizer(conn, DBI.disconnect)
        finalizer(conn, disconnect)
        conn
    else
        conn = PQsetdbLogin(host, port, C_NULL, C_NULL, db, user, passwd)
        status = PQstatus(conn)

        if status != CONNECTION_OK
            errmsg = bytestring(PQerrorMessage(conn))
            PQfinish(conn)
            error(errmsg)
        end

        PostgresDatabaseHandle(conn, status)
    end
end

function Base.connect(fn::Function, ::Type{Postgres},
                      host::AbstractString="",
                      user::AbstractString="",
                      passwd::AbstractString="",
                      db::AbstractString="",
                      port::AbstractString="")

    conn = Base.connect(Postgres, host, user, passwd, db, port)
    try
        return fn(conn)
    finally
        disconnect(conn)
    end
end

function Base.connect(fn::Function, ::Type{Postgres},
                      host::AbstractString,
                      user::AbstractString,
                      passwd::AbstractString,
                      db::AbstractString,
                      port::Integer)
    Base.connect(fn, Postgres, host, user, passwd, db, string(port))
end

function Base.connect(::Type{Postgres},
                      host::AbstractString,
                      user::AbstractString,
                      passwd::AbstractString,
                      db::AbstractString,
                      port::Integer)
    Base.connect(Postgres, host, user, passwd, db, string(port))
end

export disconnect

function disconnect(db::PostgresDatabaseHandle)
    if db.closed
        return
    else
        PQfinish(db.ptr)
        db.closed = true
        return
    end
end

function errcode(db::PostgresDatabaseHandle)
    return db.status = PQstatus(db.ptr)
end

function errstring(db::PostgresDatabaseHandle)
    return bytestring(PQerrorMessage(db.ptr))
end

function errcode(res::PostgresResultHandle)
    return PQresultStatus(res.ptr)
end

function errstring(res::PostgresResultHandle)
    return bytestring(PQresultErrorMessage(res.ptr))
end

errcode(stmt::PostgresStatementHandle) = errcode(stmt.result)
errstring(stmt::PostgresStatementHandle) = errstring(stmt.result)

export errstring, errcode, checkerr

function checkerr(result::Ptr{PGresult}, clear::Bool=true)
    status = PQresultStatus(result)
    if status == PGRES_FATAL_ERROR
        exc = PostgresQueryException(result)

        if clear
            PQclear(result)
        end

        throw(exc)
    end
    result
end

checkerr(result::PostgresResultHandle, clear::Bool=true) = begin
    checkerr(result.ptr, clear)
    result
end

checkerr(db::PostgresDatabaseHandle, clear::Bool=true) =
    checkerr(PQgetResult(db.ptr), clear)

export checkerr

function checkerrclear(result::Ptr{PGresult})
    checkerr(result)
    PQclear(result)
end



Base.run(db::PostgresDatabaseHandle, sql::AbstractString) = checkerrclear(PQexec(db.ptr, sql))

hashsql(sql::AbstractString) = bytestring(string("__", hash(sql), "__"))

function getparamtypes(result::Ptr{PGresult})
    nparams = PQnparams(result)
    return @compat [pgtype(oid(PQparamtype(result, i-1))) for i = 1:nparams]
end

const LIBC = @static is_windows() ? "msvcrt.dll" : :libc

strlen(ptr::Ptr{UInt8}) = ccall((:strlen, LIBC), Csize_t, (Ptr{UInt8},), ptr)


function pgdata{O}(t::Type{OID{O}}, ptr::Ptr{UInt8}, data)
    ptr = storestring!(ptr, pgserialize(t, data))
end

pgdata(t::Type, ptr::Ptr{UInt8}, data) = pgdata(oid(t), ptr, data)

jldata(t::Type, ptr::Ptr{UInt8}) = pgparse(oid(t), ptr)

function getparams!(ptrs::Vector{Ptr{UInt8}}, params::Vector, types::Vector, sizes::Vector, lengths, nulls::BitArray)
    fill!(nulls, false)
    for i = 1:length(ptrs)
        if params[i] in NULL_VALUES
            nulls[i] = true
        else
            ptrs[i] = pgdata(OID{Int(types[i])}, ptrs[i], params[i])
            if sizes[i] < 0
                warn("Calling strlen--this should be factored out.")
                lengths[i] = strlen(ptrs[i]) + 1
            end
        end
    end
    return
end

function cleanupparams(ptrs::Vector{Ptr{UInt8}})
    for ptr in ptrs
        Libc.free(ptr)
    end
end

export prepare, execute, executemany, finish

prepare(db::PostgresDatabaseHandle, sql::AbstractString) = PostgresStatementHandle(db, sql)

finish(stmt::PostgresStatementHandle) = nothing

function execute(stmt::PostgresStatementHandle)
    result = checkerr(PQexec(stmt.db.ptr, stmt.stmt))
    return stmt.result = PostgresResultHandle(result)
end

function execute(stmt::PostgresStatementHandle, params::Vector)
    nparams = length(params)

    if nparams > 0 && isempty(stmt.paramtypes)
        paramtypes = Oid[convert(Oid, pgoid(typeof(p))) for p in params]
    else
        paramtypes = Oid[convert(Oid, pgoid(p)) for p in stmt.paramtypes]
    end

    if nparams != length(paramtypes)
        error("Number of parameters in statement ($(length(stmt.paramtypes))) does not match number of " *
            "parameter values ($nparams).")
    end

    sizes = zeros(Int64, nparams)
    lengths = zeros(Cint, nparams)
    param_ptrs = fill(convert(Ptr{UInt8}, 0), nparams)
    nulls = falses(nparams)

    for i = 1:nparams
        sizes[i] = sizeof(paramtypes[i])

        if sizes[i] > 0
            lengths[i] = sizes[i]
        end
    end

    formats = fill(PGF_TEXT, nparams)

    getparams!(param_ptrs, params, paramtypes, sizes, lengths, nulls)

    oids = Oid[convert(Oid, oid(p)) for p in paramtypes]

    result = checkerr(PQexecParams(stmt.db.ptr, stmt.stmt, nparams, oids,
        [convert(Ptr{UInt8}, nulls[i] ? C_NULL : param_ptrs[i]) for i = 1:nparams],
        pointer(lengths), pointer(formats), PGF_TEXT))

    cleanupparams(param_ptrs)

    return stmt.result = PostgresResultHandle(result)
end

execute(db::PostgresDatabaseHandle, sql::AbstractString) =
    execute(prepare(db, sql))

execute(db::PostgresDatabaseHandle, sql::AbstractString, params::Vector) =
    execute(prepare(db, sql), params)

function executemany{T<:AbstractVector}(stmt::PostgresStatementHandle,
        params::Union{AbstractVector{T}})
    nparams = length(params[1])
    formats = fill(PGF_TEXT, nparams)
    paramtypes = stmt.paramtypes
    result = C_NULL
    rowiter = params

    for paramvec in rowiter
        if isempty(stmt.paramtypes)
            paramtypes = Type[typeof(p) for p in paramvec]
        end

        if nparams != length(paramtypes)
            error("Number of parameters in statement ($(length(paramtypes))) does not match number of " *
                "parameter values ($nparams).")
        end

        sizes = zeros(Int64, nparams)
        lengths = zeros(Cint, nparams)
        param_ptrs = fill(convert(Ptr{UInt8}, 0), nparams)
        nulls = falses(nparams)
        for i = 1:nparams
            if sizes[i] > 0
                lengths[i] = sizes[i]
            end
        end

        oids = Oid[convert(Oid, oid(p)) for p in paramtypes]
        getparams!(param_ptrs, paramvec, oids, sizes, lengths, nulls)

        if result != C_NULL
            # cleam previous result
            PQclear(result)
        end

        result = checkerr(PQexecParams(stmt.db.ptr, stmt.stmt, nparams, oids,
            [convert(Ptr{UInt8}, nulls[i] ? C_NULL : param_ptrs[i]) for i = 1:nparams],
            pointer(lengths), pointer(formats), PGF_TEXT))

        cleanupparams(param_ptrs)
    end


    stmt.result = PostgresResultHandle(result)
end

export fetchrow, unsafe_fetchrow, fetchall, import_rows

function fetchrow(stmt::PostgresStatementHandle)
    error("DBI API not fully implemented")
end

# Assumes the row exists and has the structure described in PostgresResultHandle
function unsafe_fetchrow(result::PostgresResultHandle, rownum::Integer)
    return Any[PQgetisnull(result.ptr, rownum, i-1) == 1 ? nothing :
               pgparse(datatype, PQgetvalue(result.ptr, rownum, i-1))
               for (i, datatype) in enumerate(result.types)]
end

#function unsafe_fetchcol_dataarray(result::PostgresResultHandle, colnum::Integer)
#    return @data([PQgetisnull(result.ptr, i, colnum) == 1 ? NA :
#            pgparse(result.types[colnum+1], PQgetvalue(result.ptr, i, colnum))
#            for i = 0:(PQntuples(result.ptr)-1)])
#end

function fetchall(result::PostgresResultHandle)
    return Vector{Any}[row for row in result]
end

#function fetchdf(result::PostgresResultHandle)
#    df = DataFrame()
#    for i = 0:(length(result.types)-1)
#        df[Symbol(bytestring(PQfname(result.ptr, i)))] = unsafe_fetchcol_dataarray(result, i)
#    end
#
#    return df
#end

function Base.next(result::PostgresResultHandle)
    if result.state == -1
        result.state = 0
    end

    if result.state >= result.nrows
        return nothing
    else
        state = result.state
        result.state += 1
        unsafe_fetchrow(result, state)
    end
end

function Base.length(result::PostgresResultHandle)
    return PQntuples(result.ptr)
end

function Base.start(result::PostgresResultHandle)
    return result.state = 0
end

function Base.next(result::PostgresResultHandle, state)
    return (unsafe_fetchrow(result, state), result.state += 1)
end

function Base.done(result::PostgresResultHandle, state)
    return state >= result.nrows
end

# delegate statement iteration to result
Base.start(stmt::PostgresStatementHandle) = Base.start(stmt.result)
Base.next(stmt::PostgresStatementHandle, state) = Base.next(stmt.result, state)
Base.done(stmt::PostgresStatementHandle, state) = Base.done(stmt.result, state)

function array(result::PostgresResultHandle)
    rows = Any[]
    for row in result
        push!(rows, row)
    end
    return rows
end

function on_start()
end

export array

# MOISESPSENA
function import_rows(db::PostgresDatabaseHandle, totable::AbstractString,
    columns::Tuple{AbstractString, Vararg{AbstractString}}, ds, delimiter="|"; NULL="~",
    valuefmt=(v) -> v, pre_callback=Union{Void,Function}, options...)
    push!(options, (:NULL, NULL))
    push!(options, (:DELIMITER, delimiter))
    options = join(map((v) -> "$(v[1]) $(repr(v[2]))", options),", ")

    if method_exists(Base.start, (typeof(ds),)) && method_exists(on_start, (typeof(ds),))
        on_start(ds)
    end

    copysql = "COPY $totable(" * join(columns, ", ") * ") " *
        "FROM STDIN WITH ($options)"

    function format(v::Vector)
        join([if _ == nothing NULL
              else replace(valuefmt(_), delimiter, "\\$delimiter") end
              for _ in rawvalues(v)],
            delimiter)
    end

    format(v::AbstractString) = valuefmt(v)

    _pqerr() = throw(PostgresException(db))

    function _send(data)
        data = format(data)

        if PQputCopyData(db.ptr, data, sizeof(data)) != 1
            _pqerr()
        end

        if PQputCopyData(db.ptr, "\n", 1) != 1
            _pqerr()
        end
    end

    result = checkerr(PQexec(db.ptr, copysql))

    i = 0

    if method_exists(Base.start, (typeof(ds),))
        for data in ds
            _send(data)
            i += 1
        end
    else
        while true
            data = datacb()
            if data == nothing
                break
            end
            _send(data)
            i += 1
        end
    end

    if PQputCopyEnd(db.ptr, C_NULL) != 1
        _pqerr()
    end

    PQclear(result)

    result = PQgetResult(db.ptr)

    if PGRES_COMMAND_OK != PQresultStatus(result)
        PQclear(result)
        _pqerr()
    end
end

export import_rows

function transaction(cb::Function, db::PostgresDatabaseHandle)
    run(db, "BEGIN")
    try
        r = cb()
        run(db, "COMMIT")
        return r
    catch e
        run(db, "ROLLBACK")
        rethrow()
    end
end

export transaction
