Этот проект по распределённым системам.

Задача: Пусть дан ориентированный граф, без циклов. В каждой вершине находиться функция, которую необходимо исполнить. Функция имеет право ссылаться на значения, полученные из предыдущих вершин. Необходимо за минимальное время запусить все функции.

Решение 1: Упорядочить вершины по мере независимости друг от друга и параллельно их запускать.
Решение 2: Упорядочить вершины по мере независимости друг от друга и использовать kafka.