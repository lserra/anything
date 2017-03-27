# -*- coding: utf-8 -*-
"""
http://www.python-course.eu/lambda.php
"""
"""
Lambda functions are mainly used in combination with the functions 
filter(), map() and reduce(). 
The lambda feature was added to Python due to the demand from Lisp programmers.
"""
f = lambda x, y: x + y
f(2,2)
f(10,5)

g = lambda a,b,c:(b/a)*c
g(1,2,3)
g(4,5,6)
g(1,1,1)
g(2,4,6)

h = lambda e,f,g:e-f-g
h(1,2,3)
h(3,2,1)


"""
map() is a function with two arguments:r = map(func, seq)
The first argument func is the name of a function and the second a sequence 
(e.g. a list) seq. map() applies the function func to all the elements of the 
sequence seq. It returns a new list with the elements changed by func
"""
def fahrenheit(T):
    return ((float(9)/5)*T + 32)
    
def celsius(T):
    return (float(5)/9)*(T-32)
    
temp = (4,12,-1)
    
F = map(fahrenheit, temp)
print(F)

C = map(celsius, F)
print(C)

F = map(lambda C:(float(5)/9)*(C-32), C)
print(F)

C = map(lambda F:((float(9)/5)*F + 32), F)
print(C)

a = [1,2,3,4]
b = [17,12,11,10]
c = [-1,-4,5,9]
map(lambda x: x*2, a)
map(lambda x: x*3, b)
map(lambda x: x*4, c)
map(lambda x,y: x+y, a,b)
map(lambda x, y, z: x*y*z, a,b,c)


"""
The function filter(function, list) offers an elegant way to filter out all 
the elements of a list, for which the function function returns True. 
The function filter(f,l) needs a function f as its first argument. f returns 
a Boolean value, i.e. either True or False. This function will be applied to 
every element of the list l. Only if f returns True will the element of the 
list be included in the result list.
"""
fib = [0,1,1,2,3,5,8,13,21,34,55]
result = filter(lambda x: x%2, fib)
print(result)

result = filter(lambda x: x%2==0, fib)
print(result)

name = ['l','a','e','r','c','i','o']
result = filter(lambda x:x=='e', name)
print (result)


"""
The function reduce(func, seq) continually applies the function func() to the 
sequence seq. It returns a single value.
"""
f = lambda a,b: a if (a > b) else b
reduce(f, [47,11,42,102,13])

f = lambda a,b: a+b
reduce(f, [47,11,42,102,13])
reduce(f, range(1,101))
