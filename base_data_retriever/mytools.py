def class_partial(method,*args,**kwargs):
    def undotted(instance):
        return getattr(instance,method)(*args,**kwargs)
    return undotted
