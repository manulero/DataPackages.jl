isdefined(Base, :__precompile__) && __precompile__()

module DataPackages

using PyCall
const dp = PyNULL()

export infer_datapackage_descriptor,
    create_datapackage_from_descriptor,
    save_datapackage_to_disk,
    add_foreign_key!,
    set_primary_key!,
    set_field_attribute!

# Datapackage interface
infer_datapackage_descriptor(path::String) = dp[:infer](joinpath(path, "*.csv"), path)
create_datapackage_from_descriptor(pkg_desc::Dict) = dp[:Package](pkg_desc)
save_datapackage_to_disk(package::PyObject, path::String) = package[:save](joinpath(path, "datapackage.json"))

function save_datapackage_to_disk(pkg_desc::Dict, path::String)
    package = create_datapackage_from_descriptor(pkg_desc)
    save_datapackage_to_disk(package, path)
end

function get_resource_descriptor(pkg_desc::Dict, rsrc_name::String)
    filter(x -> x["name"] == rsrc_name, pkg_desc["resources"])[]
end

function add_foreign_key!(schema_desc::Dict, foreign_key::Dict)
    !haskey(schema_desc, "foreignKeys") && (schema_desc["foreignKeys"] = Array{Any,1}())
    push!(schema_desc["foreignKeys"], foreign_key)
end

function add_foreign_key!(pkg_desc::Dict, rsrc_name::String, foreign_key::Dict)
    rsrc_desc = get_resource_descriptor(pkg_desc, rsrc_name)
    add_foreign_key!(rsrc_desc["schema"], foreign_key)
end

function add_foreign_key!(package::PyObject, rsrc_name::String, foreign_key::Dict)
    pkg_desc = Dict(package[:descriptor])
    add_foreign_key!(pkg_desc, rsrc_name, foreign_key)
    package = dp[:Package](pkg_desc)
end

function add_foreign_key!(package, rsrc::String, fields::Array{String,1}, ref_rsrc::String, ref_fields::Array{String,1})
    foreign_key = Dict(
        "fields" => fields,
        "reference" => Dict(
            "resource" => ref_rsrc,
            "fields" => ref_fields
        )
    )
    add_foreign_key!(package, rsrc, foreign_key)
end

function set_primary_key!(schema_desc::Dict, primary_key::Array{String,1})
    schema_desc["primaryKey"] = primary_key
end

function set_primary_key!(pkg_desc::Dict, rsrc_name::String, primary_key::Array{String,1})
    rsrc_desc = get_resource_descriptor(pkg_desc, rsrc_name)
    set_primary_key!(rsrc_desc["schema"], primary_key)
end

function set_primary_key!(package::PyObject, rsrc_name::String, primary_key::Array{String,1})
    pkg_desc = Dict(package[:descriptor])
    set_primary_key!(pkg_desc, rsrc_name, primary_key)
    package = dp[:Package](pkg_desc)
end

function get_field_descriptor(schema_desc::Dict, field_name::String)
    filter(x -> x["name"] == field_name, schema_desc["fields"])[]
end

function set_field_attribute!(schema_desc::Dict, field_name::String, attr_name::String, attr_val::String)
    field_desc = get_field_descriptor(schema_desc, field_name)
    field_desc[attr_name] = attr_val
end

function set_field_attribute!(pkg_desc::Dict, rsrc_name::String, field_name::String, attr_name::String, attr_val::String)
    rsrc_desc = get_resource_descriptor(pkg_desc, rsrc_name)
    set_field_attribute!(rsrc_desc["schema"], field_name::String, attr_name::String, attr_val::String)
end

function set_field_attribute!(package::PyObject, rsrc_name::String, field_name::String, attr_name::String, attr_val::String)
    pkg_desc = Dict(package[:descriptor])
    set_field_attribute!(pkg_desc, field_name::String, attr_name::String, attr_val::String)
    package = dp[:Package](pkg_desc)
end

function set_field_attribute!(package, rsrc_name::String, field_names::Array{String,1}, attr_name::String, attr_val::String)
    for field_name in field_names
        set_field_attribute!(package, rsrc_name, field_name, attr_name, attr_val)
    end
end

function __init__()
    copy!(dp, pyimport("datapackage"))
end

end
 # module
