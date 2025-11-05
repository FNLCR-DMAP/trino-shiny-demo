package trino

# deny by default
default allow = false
default rowFilters = []
default columnMask = []

# Admin can do anything (no action/resource required)
allow {
  input.context.identity.user == "admin"
}

