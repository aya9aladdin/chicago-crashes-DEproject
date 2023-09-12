 {#
    This macro returns day time of crash damage 
#}

{% macro day_time(hour) -%}
   case
    when  {{hour}}  < 4 then 'midnight' 
    when {{hour}}  < 9 then 'early morning'
    when  {{hour}}  < 12 then 'morning' 
    when {{hour}}  < 15 then 'afternoon'
    when {{hour}}  < 19 then 'evening'
    when {{hour}}  < 24 then 'night'
   end
{%- endmacro %}

              