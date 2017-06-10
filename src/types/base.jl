export JSONB

type NULL end

abstract AbstractPostgresType
type PostgresType{Name} <: AbstractPostgresType end

abstract AbstractOID
type OID{N} <: AbstractOID end

type JSONB
    data::Union{Dict,Array}
end

const OID_BY_PGTYPE = Dict()
const PGTYPE_BY_OID = Dict()
const PGTYPE_BY_JLTYPE = Dict()

oid{S<:Signed}(v::S) = OID{v}
oid{T}(pt::Type{PostgresType{T}}) = OID_BY_PGTYPE[pt]
oid(tname::Symbol) = oid(PostgresType{tname})
oid(v::UInt32) = OID{Int(v)}
oidt{T}(t::Type{OID{T}}) = Type{t}
oidt{S<:Signed}(v::S) = oidt(OID{v})
oidt(v::UInt32) = oidt(OID{Int(v)})
oidt(tname::Symbol) = oidt(oid(tname))

function pgserialize() end
function pgparse() end

function newpgtype{T,U}(pt::Type{PostgresType{T}}, oidt::Type{OID{U}})
    if haskey(OID_BY_PGTYPE, pt)
        error("overwritten $pt of $(OID_BY_PGTYPE[pt]) to $oidt")
    end

    OID_BY_PGTYPE[pt] = oidt
    PGTYPE_BY_OID[oidt] = pt

    oid, pt
end

newpgtype(tname::Symbol, oid::Signed; kwargs...) =
    newpgtype(PostgresType{tname}, OID{oid}; kwargs...)

function jltyperegister{O}(oid::Type{OID{O}}, t::Type)
    if !haskey(PGTYPE_BY_JLTYPE, t)
        PGTYPE_BY_JLTYPE[t] = Any[]
    end

    push!(PGTYPE_BY_JLTYPE[t], oid)
    t
end

jltyperegister(tname::Symbol, t::Type) = jltyperegister(oid(tname), t)

pgtype(t::Type) = supertype(t) == AbstractOID ? PGTYPE_BY_OID[t] : PGTYPE_BY_JLTYPE[t][end]

const rjl = jltyperegister


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

pgisnull(v) = v === nothing || v === NA || v === Union{}


function rawvalue{T}(v::T, NULL=nothing)
    if pgisnull(v)
        NULL
    else
        pgserialize(pgtype(T), v)
    end
end

function rawvalues(values::Vector, NULL=nothing)
    Any[rawvalue(values[i]) for i = 1:length(values)]
end

export rawvalue, rawvalues

function storestring!(ptr::Ptr{UInt8}, str::String)
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