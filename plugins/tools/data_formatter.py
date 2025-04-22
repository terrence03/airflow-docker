from decimal import Decimal, ROUND_HALF_UP


def format_float(x: float) -> Decimal:
    """Format float to Decimal with 2 decimal places.

    Parameters
    ----------
    x : float
        A float number.

    Returns
    -------
    Decimal
        A Decimal number with 2 decimal places.

    Examples
    --------
    >>> format_float(1.2345)
    Decimal('1.23')
    >>> format_float(1.2355)
    Decimal('1.24')
    """
    return Decimal(x).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)


def format_percent(x: float) -> str:
    """Format float to percentage with 2 decimal places.

    Parameters
    ----------
    x : float
        A float number.

    Returns
    -------
    str
        A percentage string with 2 decimal places.

    Examples
    --------
    >>> format_percent(0.12345)
    '12.35%'
    >>> format_percent(0.12355)
    '12.36%'
    """
    return f"{Decimal(x).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP):.2%}"


def format_simbol(x: float | str) -> str:
    """Format number to string with symbol.

    Parameters
    ----------
    x : float | str
        A float or string number, sting number should be end with '%'.

    Returns
    -------
    str
        A string number with symbol.

    Examples
    --------
    >>> format_simbol(1.23)
    '▲ 1.23'
    >>> format_simbol(-1.23)
    '▼ 1.23'
    >>> format_simbol(0)
    '--'
    >>> format_simbol('1.23%')
    '▲ 1.23%
    >>> format_simbol('-1.23%')
    '▼ 1.23%'
    """
    if isinstance(x, float):
        if x > 0:
            return f"▲ {x}".replace("-", "")
        elif x < 0:
            return f"▼ {x}"
        else:
            return "--"
    if isinstance(x, str):
        if float(x[:-1]) == 0:
            return "--"
        elif x[0] == "-":
            return f"▼ {x}".replace("-", "")
        else:
            return f"▲ {x}"
