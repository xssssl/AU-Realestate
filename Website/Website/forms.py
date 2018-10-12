from django import forms

class analysisperiod(forms.Form):
    period_choices = [('14days','14 days'),('30days','30 days'), ('60days','60 days'),
                      ('90days','90 days'), ('180days', '180 days')]
    Period = forms.ChoiceField(choices=period_choices)

