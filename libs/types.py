import datetime


def isString(var):
    return isinstance(var, str)


def isInteger(var):
    return isinstance(var, int)


def isFloat(var):
    return isinstance(var, float)


def isDict(var):
    return isinstance(var, dict)


def isList(var):
    return isinstance(var, list)


def isTuple(var):
    return isinstance(var, tuple)


def isObject(var):
    return isinstance(var, object)


def isDate(var):
    return isinstance(var, datetime.date)
