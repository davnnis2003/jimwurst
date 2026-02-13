{{ config(materialized='view', 
tags=['staging', 'linkedin', 'basic_content']) }}

select * from {{ source('linkedin', 'basic_content_engagement') }}
