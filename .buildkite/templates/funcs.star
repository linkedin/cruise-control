load("@ytt:struct", "struct")


def get_message (is_deployment, target):
  if is_deployment:
    return "Packaging and publishing chart {}".format(target)
  else:
    return "Testing chart {}".format(target)
  end
end

utils = struct.make(get_message=get_message)