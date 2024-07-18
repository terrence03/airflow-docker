import calendar
from datetime import datetime, timedelta


class PeriodModel:
    """Period

    Represents a specific period of time defined by a year and a month.

    Parameters
    ----------
    year : int
        The year of the period. Must be greater than 2003.
    month : int
        The month of the period. Must be between 1 and 12.

    """

    def __init__(self, year: int, month: int) -> None:
        assert year >= 2003, "year must be greater than 2003"
        assert 1 <= month <= 12, "month must be between 1 and 12"
        self.year = year
        self.month = month

    def __str__(self) -> str:
        return f"{self.year}-{self.month:02d}"

    def __repr__(self) -> str:
        return f"{self.year}-{self.month:02d}"

    @property
    def string(self) -> str:
        return f"{self.year}-{self.month:02d}"

    def get_date_range(self) -> tuple[str, str]:
        """Get the date range of the period.

        Returns
        -------
        tuple[str, str]
            The start and end date of the period.

        """
        start = datetime(self.year, self.month, 1).strftime("%Y-%m-%d")
        end = datetime(
            self.year, self.month, calendar.monthrange(self.year, self.month)[1]
        ).strftime("%Y-%m-%d")
        return start, end

    def get_calendar_date_range(self) -> tuple[str, str]:
        """Get the calendar date range of the period.

        Returns
        -------
        tuple[datetime, datetime]
            The start and end date of the period.

        """
        sunday_first_calendar = calendar.Calendar(firstweekday=calendar.SUNDAY)
        month_dates = sunday_first_calendar.monthdatescalendar(self.year, self.month)
        start = month_dates[0][0]
        end = month_dates[-1][-1]
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


class Period(PeriodModel):
    """Period set

    Represents a set of periods.

    Parameters
    ----------
    year : int, optional
        The year of the period set. If not provided, the current year is used.
    month : int, optional
        The month of the period set. If not provided, the current month is used.

    """

    def __init__(self, year: int = None, month: int = None) -> None:
        assert all([year, month]) or not any(
            [year, month]
        ), "year and month must be both None or not None"

        if all([year, month]):
            assert year >= 2003, "year must be greater than 2003"
            assert 1 <= month <= 12, "month must be between 1 and 12"
            self.year = year
            self.month = month

        if not any([year, month]):
            today = datetime.today()
            self.year = today.year
            self.month = today.month

    def shift_month(self, month: int) -> PeriodModel:
        """Shift the period by a specified number of months.

        Parameters
        ----------
        month : int
            The number of months to shift the period, can be positive or negative.

        Returns
        -------
        Period
            The shifted period.

        Examples
        --------
        >>> Period(2024, 1).shift_month(1)
        Period(2024, 2)
        >>> Period(2024, 1).shift_month(-1)
        Period(2023, 12)
        """
        year = self.year + (self.month + month - 1) // 12
        month = (self.month + month - 1) % 12 + 1
        return PeriodModel(year=year, month=month)

    @property
    def this_month(self) -> PeriodModel:
        """Get the current month.

        Returns
        -------
        Period
            The current period.
        """
        return PeriodModel(year=self.year, month=self.month)

    @property
    def previous_month(self) -> PeriodModel:
        """Get the previous month.

        Returns
        -------
        Period
            The previous period.
        """
        return self.shift_month(-1)

    @property
    def previous_2_month(self) -> PeriodModel:
        """Get the month before the previous month.

        Returns
        -------
        Period
            The period before the previous period.

        """
        return self.shift_month(-2)

    @property
    def same_month_last_year(self) -> PeriodModel:
        """Get the period in the same month of the previous year.

        Returns
        -------
        Period
            The period in the same month of the previous year.
        """
        return self.shift_month(-12)

    @property
    def nearly_6_month(self) -> list[PeriodModel]:
        """Get the periods of the previous 6 months.

        Returns
        -------
        list[Period]
            The periods of the previous 6 months.
        """
        periods = []
        for i in range(6):
            periods.append(self.shift_month(-i))
        return periods


class Week:
    def __init__(self, week_first_day: str = None) -> None:
        if week_first_day == "Sunday":
            self.cald = calendar.Calendar(firstweekday=calendar.SUNDAY)
        elif week_first_day == "Monday" or week_first_day is None:
            self.cald = calendar.Calendar(firstweekday=calendar.MONDAY)
        else:
            raise ValueError("week_first_day must be 'Sunday' or 'Monday'")

    def get_week_range(self, date: datetime = None) -> tuple[datetime, datetime]:
        if date is None:
            date = datetime.today().date()
        cald = self.cald
        month_dates = cald.monthdatescalendar(date.year, date.month)
        for week in month_dates:
            if date in week:
                start = week[0]
                end = week[-1]
                break
        return start, end

    def get_week_id(self, date: datetime = None) -> int:
        base_week = datetime(2003, 1, 5).date()
        base_week_id = 158
        if date is None:
            date = datetime.today()
        date_diff = date.date() - base_week
        week_id = base_week_id + date_diff.days // 7
        return week_id

    def get_week_dates(self, date: datetime = None) -> list[datetime]:
        if date is None:
            date = datetime.today()
        start, end = self.get_week_range(date)
        week_dates = [start + timedelta(days=i) for i in range(7)]
        return week_dates


class Month:
    def __init__(self) -> None:
        self.cald = calendar.Calendar()

    def get_month_range(
        self, year: int = None, month: int = None
    ) -> tuple[datetime, datetime]:
        if all([year, month]):
            start = datetime(year, month, 1)
            end = datetime(year, month, calendar.monthrange(year, month)[1])
        else:
            today = datetime.today()
            start = datetime(today.year, today.month, 1)
            end = datetime(
                today.year, today.month, calendar.monthrange(today.year, today.month)[1]
            )
        return start, end

    def get_month_dates(self, year: int = None, month: int = None) -> list[datetime]:
        if all([year, month]):
            month_dates = self.cald.monthdatescalendar(year, month)
        else:
            today = datetime.today()
            year = today.year
            month = today.month
            month_dates = self.cald.monthdatescalendar(year, month)

        month_dates = [date for week in month_dates for date in week]
        month_dates = [date for date in month_dates if date.month == month]
        return month_dates

    def last_month(self) -> tuple[int, int]:
        today = datetime.today()
        last_month = today.replace(day=1) - timedelta(days=1)
        return last_month.year, last_month.month
