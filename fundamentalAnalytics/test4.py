from sympy.core.symbol import symbols


x, y = symbols('x, y')
expr = (x / y) - 1
print(expr.free_symbols)