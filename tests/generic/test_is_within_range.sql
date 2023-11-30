{% test is_within_range(model, column_name, lower_bound, upper_bound) %}

SELECT
    {{ column_name }}

FROM {{ model }}

WHERE {{ column_name }} NOT BETWEEN {{ lower_bound }} AND {{ upper_bound }}

{% endtest %}