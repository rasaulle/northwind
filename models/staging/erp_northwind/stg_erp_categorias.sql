with
    fonte_categorias as (
        select
            cast(category_id as int) as id_categoria
            , cast(category_name as string) as nome_categoria
            , cast(description as string) as descricao
        from {{ source('erp', 'categories') }}
    )

select *
from fonte_categorias