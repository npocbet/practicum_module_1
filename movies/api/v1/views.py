from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movies.models import FilmWork, PersonFilmwork


class MoviesApiMixin:
    model = FilmWork
    http_method_names = ['get']
    paginate_by = 50

    def get_queryset(self):
        films = FilmWork.objects.prefetch_related('genres', 'persons', ) \
            .annotate(genres=ArrayAgg('genre__name', distinct=True),
                      actors=ArrayAgg('person__full_name', filter=Q(personfilmwork__role=PersonFilmwork.Role.a),
                                      distinct=True),
                      directors=ArrayAgg('person__full_name', filter=Q(personfilmwork__role=PersonFilmwork.Role.d),
                                         distinct=True),
                      writers=ArrayAgg('person__full_name', filter=Q(personfilmwork__role=PersonFilmwork.Role.w),
                                       distinct=True)) \
            .values('id', 'title', 'description', 'creation_date', 'rating', 'type', 'genres',
                    'actors', 'directors', 'writers')
        return films


    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset,
            self.paginate_by
        )
        return {'count': paginator.count,
                'total_pages': paginator.num_pages,
                'prev': page.previous_page_number() if page.has_previous() else None,
                'next': page.next_page_number() if page.has_next() else None,
                'results': list(page.object_list.values('id', 'title', 'description', 'creation_date',
                                                        'rating', 'type', 'genres', 'actors',
                                                        'directors', 'writers'))
                }


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):

    def get_context_data(self, **kwargs):

        return {'id': kwargs['object']['id'],
                'title': kwargs['object']['title'],
                'description': kwargs['object']['description'],
                'creation_date': kwargs['object']['creation_date'],
                'rating': kwargs['object']['rating'],
                'type': kwargs['object']['type'],
                'genres': kwargs['object']['genres'],
                'actors': kwargs['object']['actors'],
                'directors': kwargs['object']['directors'],
                'writers': kwargs['object']['writers']
                }
