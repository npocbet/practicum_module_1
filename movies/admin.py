from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmwork, PersonFilmwork, Person


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ['genre']


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ['person']


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ['genre']


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ['person']


@admin.register(FilmWork)
class FilmworkAdmin(admin.ModelAdmin):

    def get_genres(self, obj):
        return ','.join([g.name for g in obj.genre.all()])

    get_genres.short_description = 'Жанры фильма'

    inlines = (GenreFilmworkInline, PersonFilmworkInline)
    # Отображение полей в списке
    list_display = ('title', 'type', 'creation_date', 'rating', 'get_genres',)
    # Фильтрация в списке
    list_filter = ('type',)
    # Поиск по полям
    search_fields = ('title', 'description', 'id')
