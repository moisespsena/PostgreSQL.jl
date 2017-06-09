import Compat: Libc, @compat

function Base.connect(::Type{Postgres},
                      host::AbstractString="",
                      user::AbstractString="",
                      passwd::AbstractString="",
                      db::AbstractString="",
                      port::AbstractString="")
    conn = PQsetdbLogin(host, port, C_NULL, C_NULL, db, user, passwd)
    status = PQstatus(conn)

    if status != CONNECTION_OK
        errmsg = bytestring(PQerrorMessage(conn))
        PQfinish(conn)
        error(errmsg)
    end

    conn = PostgresDatabaseHandle(conn, status)
    return conn
end

function Base.connect(::Type{Postgres},
                      host::AbstractString,
                      user::AbstractString,
                      passwd::AbstractString,
                      db::AbstractString,
                      port::Integer)
    Base.connect(Postgres, host, user, passwd, db, string(port))
end

# Note that for some reason, `do conn` notation
# doesn't work using this version of the function
function Base.connect(::Type{Postgres};
                      dsn::AbstractString="")
    conn = PQconnectdb(dsn)
    status = PQstatus(conn)
    if status != CONNECTION_OK
        errmsg = bytestring(PQerrorMessage(conn))
        PQfinish(conn)
        error(errmsg)
    end
    conn = PostgresDatabaseHandle(conn, status)
    finalizer(conn, DBI.disconnect)
    return conn
end

function DBI.disconnect(db::PostgresDatabaseHandle)
    if db.closed
        return
    else
        PQfinish(db.ptr)
        db.closed = true
        return
    end
end

function DBI.errcode(db::PostgresDatabaseHandle)
    return db.status = PQstatus(db.ptr)
end

function DBI.errstring(db::PostgresDatabaseHandle)
    return bytestring(PQerrorMessage(db.ptr))
end

function DBI.errcode(res::PostgresResultHandle)
    return PQresultStatus(res.ptr)
end

function DBI.errstring(res::PostgresResultHandle)
    return bytestring(PQresultErrorMessage(res.ptr))
end

DBI.errcode(stmt::PostgresStatementHandle) = DBI.errcode(stmt.result)
DBI.errstring(stmt::PostgresStatementHandle) = DBI.errstring(stmt.result)

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

escapeliteral(db::PostgresDatabaseHandle, value) = value
escapeliteral(db::PostgresDatabaseHandle, value::AbstractString) = escapeliteral(db, bytestring(value))

function escapeliteral(db::PostgresDatabaseHandle, value::Union{ASCIIString, UTF8String})
    strptr = PQescapeLiteral(db.ptr, value, sizeof(value))
    str = bytestring(strptr)
    PQfreemem(strptr)
    return str
end

function escape(val)
  if val == nothing return "NULL"
  elseif isa(val, Vector)
    vals = map(escape, val)
    return "ARRAY[" * join(vals, ", ") * "]"
  elseif isa(val, Tuple)
    vals = map(escape, val)
    return "(" * join(vals, ", ") * ")"
  elseif isa(val, AbstractString)
      prefix = search(val, '\\') > 0 ? 'E' : ""
      val = replace(val, '\'', "''")
      val = replace(val, '\\', "\\\\")
      return string(prefix, "'", val, "'")
  end
  string(val)
end

Base.run(db::PostgresDatabaseHandle, sql::AbstractString) = checkerrclear(PQexec(db.ptr, sql))

hashsql(sql::AbstractString) = bytestring(string("__", hash(sql), "__"))

function getparamtypes(result::Ptr{PGresult})
    nparams = PQnparams(result)
    return @compat [pgtype(OID{Int(PQparamtype(result, i-1))}) for i = 1:nparams]
end

LIBC = @windows ? "msvcrt.dll" : :libc
strlen(ptr::Ptr{UInt8}) = ccall((:strlen, LIBC), Csize_t, (Ptr{UInt8},), ptr)

function getparams!(ptrs::Vector{Ptr{UInt8}}, params, types, sizes, lengths::Vector{Int32}, nulls)
    fill!(nulls, false)
    for i = 1:length(ptrs)
        if params[i] === nothing || params[i] === NA || params[i] === Union{}
            nulls[i] = true
        else
            ptrs[i] = pgdata(types[i], ptrs[i], params[i])
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

DBI.prepare(db::PostgresDatabaseHandle, sql::AbstractString) = PostgresStatementHandle(db, sql)

DBI.finish(stmt::PostgresStatementHandle) = nothing

function DBI.execute(stmt::PostgresStatementHandle)
    result = checkerr(PQexec(stmt.db.ptr, stmt.stmt))
    return stmt.result = PostgresResultHandle(result)
end

pgisnull(v) = v === nothing || v === NA || v === Union{}

function rawvalue{T}(v::T, NULL=nothing)
    t = pgtype(T)

    if pgisnull(v)
        NULL
    else
        pgdataraw(t, v)
    end
end

function rawvalues(values::Vector, NULL=nothing)
    Any[rawvalue(values[i]) for i = 1:length(values)]
end

function escaped_rawvalue(v)
    v == nothing ? "NULL" : escapeliteral(rawvalue(v))
end

export rawvalue, rawvalues

function DBI.execute(stmt::PostgresStatementHandle, params::Vector)
    nparams = length(params)

    if nparams > 0 && isempty(stmt.paramtypes)
        paramtypes = [pgtype(typeof(p)) for p in params]
    else
        paramtypes = stmt.paramtypes
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
        if paramtypes[i] === nothing
            paramtypes[i] = pgtype(typeof(params[i]))
        end

        sizes[i] = sizeof(paramtypes[i])

        if sizes[i] > 0
            lengths[i] = sizes[i]
        end
    end

    formats = fill(PGF_TEXT, nparams)

    getparams!(param_ptrs, params, paramtypes, sizes, lengths, nulls)

    oids = Oid[convert(Oid, oid(p)) for p in paramtypes]

    result = checkerr(PQexecParams(stmt.db.ptr, stmt.stmt, nparams,
        oids,
        [convert(Ptr{UInt8}, nulls[i] ? C_NULL : param_ptrs[i]) for i = 1:nparams],
        pointer(lengths), pointer(formats), PGF_TEXT))

    cleanupparams(param_ptrs)

    return stmt.result = PostgresResultHandle(result)
end

DBI.execute(db::PostgresDatabaseHandle, sql::AbstractString) =
    execute(prepare(db, sql))

DBI.execute(db::PostgresDatabaseHandle, sql::AbstractString, params::Vector) =
    execute(prepare(db, sql), params)

function executemany{T<:AbstractVector}(stmt::PostgresStatementHandle,
        params::Union{DataFrame,AbstractVector{T}})
    nparams = isa(params, DataFrame) ? ncol(params) : length(params[1])

    if nparams > 0 && isempty(stmt.paramtypes)
        if isa(params, DataFrame)
            paramtypes = collect(PostgresType, eltypes(params))
        else
            paramtypes = [pgtype(typeof(p)) for p in params[1]]
        end
    else
        paramtypes = stmt.paramtypes
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
        if paramtypes[i] === nothing
            paramtypes[i] = pgtype(typeof(params[1][i]))
        end

        if sizes[i] > 0
            lengths[i] = sizes[i]
        end
    end
    formats = fill(PGF_TEXT, nparams)

    result = C_NULL
    rowiter = isa(params, DataFrame) ? eachrow(params) : params

    for paramvec in rowiter
        getparams!(param_ptrs, paramvec, paramtypes, sizes, lengths, nulls)

        oids = Oid[convert(Oid, oid(p)) for p in paramtypes]

        if result != C_NULL
            # cleam previous result
            PQclear(result)
        end

        result = checkerr(PQexecParams(stmt.db.ptr, stmt.stmt, nparams,
            oids,
            [convert(Ptr{UInt8}, nulls[i] ? C_NULL : param_ptrs[i]) for i = 1:nparams],
            pointer(lengths), pointer(formats), PGF_TEXT))
    end

    cleanupparams(param_ptrs)

    stmt.result = PostgresResultHandle(result)
end

function DBI.fetchrow(stmt::PostgresStatementHandle)
    error("DBI API not fully implemented")
end

# Assumes the row exists and has the structure described in PostgresResultHandle
function unsafe_fetchrow(result::PostgresResultHandle, rownum::Integer)
    return Any[PQgetisnull(result.ptr, rownum, i-1) == 1 ? nothing :
               jldata(datatype, PQgetvalue(result.ptr, rownum, i-1))
               for (i, datatype) in enumerate(result.types)]
end

function unsafe_fetchcol_dataarray(result::PostgresResultHandle, colnum::Integer)
    return @data([PQgetisnull(result.ptr, i, colnum) == 1 ? NA :
            jldata(result.types[colnum+1], PQgetvalue(result.ptr, i, colnum))
            for i = 0:(PQntuples(result.ptr)-1)])
end

function DBI.fetchall(result::PostgresResultHandle)
    return Vector{Any}[row for row in result]
end

function DBI.fetchdf(result::PostgresResultHandle)
    df = DataFrame()
    for i = 0:(length(result.types)-1)
        df[symbol(bytestring(PQfname(result.ptr, i)))] = unsafe_fetchcol_dataarray(result, i)
    end

    return df
end

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