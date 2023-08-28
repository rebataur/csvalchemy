from django import forms

class UploadFileForm(forms.Form):
    name = forms.CharField(max_length=50)
    csv_file = forms.FileField()

class UploadFileDataForm(forms.Form):   
    csv_file = forms.FileField(widget=forms.ClearableFileInput(attrs={'multiple': True}),required=False)
    folder_path = forms.CharField(max_length=50,required=False)
    


class UploadFunctionForm(forms.Form):   
    function_file = forms.FileField(widget=forms.ClearableFileInput(attrs={'multiple': True}))

class DerivedColumnForm(forms.Form):
    name = forms.CharField(max_length=50)