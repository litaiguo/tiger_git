#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 生成 0 ~ 9 之间的随机数

# 导入 random(随机数) 模块
import random

a=random.randint(0, 9)
print(a)

'''

b=random.uniform(1.1,9.8)
print(b)

c=[1,2,3,4,5,6,'a','b','c','d','e']
d=random.choice(c)
print(d)

e=random.randrange(1,10)
print(e)

'''

# s=list('abcdefg')
# print(s)
# random.shuffle(s)
# print(s)

'''
s=list('abcdefg')
print(s)
for i in range(1,5):
    random.shuffle(s)
    print(s)
'''

# g=random.sample([1,2,3,4,5,6,7],3)
# print(g)
# print(random.sample([1,2,3,4,5,6,7],3))


# c=[1,2,3,4,5,6,'a','b','c','d','e']
# for i in range(1,5):
#     print(random.choice(c),end=' ')



'''
res=[]
for i in range(0,100):
    if i % 3 == 1:
        res.append(i)
print(res)
'''