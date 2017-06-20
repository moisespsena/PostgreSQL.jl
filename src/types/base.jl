export JSONB, @jsonb_str, pgtype, oidt, oidt, pgname, jltype, pgoid

type NULL end

abstract AbstractPostgresType
type PostgresType{Name} <: AbstractPostgresType end

abstract AbstractOID
type OID{N} <: AbstractOID end

type JSONB
    data::Union{Dict,Array}
end

macro jsonb_str(s)
    return JSONB(s)
end

function pgserialize() end
function pgparse() end
function pgname() end
pgoid(name::Symbol) = pgoid(PostgresType{name})

oid{S<:Signed}(v::S) = OID{v}
oid{T}(pt::Type{PostgresType{T}}) = OID{pgoid(T)}
oid(tname::Symbol) = oid(PostgresType{tname})
oid(v::UInt32) = OID{Int(v)}
oid(v::Vector) = map(oid, v)
oidt{T}(t::Type{OID{T}}) = Type{t}
oidt{S<:Signed}(v::S) = oidt(OID{v})
oidt(v::UInt32) = oidt(OID{Int(v)})
oidt(tname::Symbol) = oidt(oid(tname))
pgtype(tname::Symbol) = PostgresType{tname}
function jltype() end

# const NULL_VALUES = (nothing, Union{}, NA)
const NULL_VALUES = (nothing, Union{})
pgisnull(v) = v == nothing || v == Union{}

function newpgtype{T,U}(pt::Type{PostgresType{T}}, oidt::Type{OID{U}})
    @eval pgname(::Type{$oidt}) = next($pt.parameters, 1)[1]
    @eval pgtype(::Type{$oidt}) = PostgresType{next($pt.parameters, 1)[1]}
    @eval pgoid(::Type{$pt}) = $U
    oid, pt
end

newpgtype(tname::Symbol, oid::Signed; kwargs...) =
    newpgtype(PostgresType{tname}, OID{oid}; kwargs...)

function jltyperegister{O}(oidt::Type{OID{O}}, types::Type...)
    for t=types
        if ! method_exists(oid, (Type{t},))
            @eval oid(::Type{$t}) = $oidt
        end

        if ! method_exists(pgoid, (Type{t},))
            @eval pgoid(::Type{$t}) = $O
        end

        if ! method_exists(jltype, (Type{oidt},))
            @eval jltype(::Type{$oidt}) = $t
        end

        if ! method_exists(pgtype, (Type{t},))
            @eval pgtype(t::Type{$t}) = pgtype(oid(t))
            @eval pgname(t::Type{$t}) = pgname(oid(t))
        end
    end
    types[1]
end

jltyperegister(tname::Symbol, t::Type) = jltyperegister(oid(tname), t)

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

rawvalue{T}(v::T, NULL=nothing) = pgisnull(v) ? NULL : pgserialize(oid(typeof(v)), v)
rawvalues(values::Vector, null=nothing) = map(v -> rawvalue(v, null), values)
rawrow(values::Vector; sep="\t", null="NULL", fmt=(_) -> _) =
    join(
        map(
            c -> c == nothing ? null : replace(fmt(c), sep, "\\$sep"),
            rawvalues(values)
        ),
        sep
    )

export rawvalue, rawvalues, rawrow

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
