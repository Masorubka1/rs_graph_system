Decomposition and dispatching of tasks in a parallel computing environment.

Problem: Let be given a directed graph, without cycles. Each vertex contains a function that needs to be executed. The function has the right to refer to the values obtained from the previous vertices. It is necessary to run all functions in a minimum time.

Solution:
1) Arrange the vertices as they are independent of each other
2*) run tasks in parallel (branch main)
2) run tasks in parallel with usage kafka (branch kafka)

run/test ...

Этот проект по распределённым системам.

Задача: Пусть дан ориентированный граф, без циклов. В каждой вершине находиться функция, которую необходимо исполнить. Функция имеет право ссылаться на значения, полученные из предыдущих вершин. Необходимо за минимальное время запусить все функции.

Решение 1: Упорядочить вершины по мере независимости друг от друга и параллельно их запускать.
Решение 2: Упорядочить вершины по мере независимости друг от друга и использовать kafka.
