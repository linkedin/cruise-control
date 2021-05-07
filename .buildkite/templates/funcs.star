load("@ytt:struct", "struct")

def get_build_type(changes, triggers):

  if changes["force"]:
    print("override change detection, build all")
    return "both"
  end

  changed = set(changes["folders"])
  code = set(triggers.container_build)
  helm = set(triggers.helm_build)

  code_build = changed & code
  helm_build = changed & helm

  if len(code_build) > 0 and len(helm_build) > 0:
    print("deploy both")
    return "both"
  elif len(helm_build) > 0:
    print("build helm only")
    return "helm"
  elif len(code_build) > 0:
    print("build container")
    return "code"
  else:
    return "ignore"
  end
end

utils = struct.make(get_build_type=get_build_type)