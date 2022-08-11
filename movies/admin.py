from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


@admin.register(FilmWork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline,)
    # Отображение полей в списке
    list_display = ('title', 'type', 'creation_date', 'rating',)
    # Фильтрация в списке
    list_filter = ('type',)
    # Поиск по полям
    search_fields = ('title', 'description', 'id')
