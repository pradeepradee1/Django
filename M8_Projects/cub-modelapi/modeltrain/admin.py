from django.contrib import admin
from .models import TrainAudit, TransposeAudit,CleanserAudit,EncoderAudit,ScalarAudit,TestAudit

# Register your models here.

admin.site.register(TransposeAudit)
admin.site.register(CleanserAudit)
admin.site.register(EncoderAudit)
admin.site.register(ScalarAudit)
admin.site.register(TrainAudit)
admin.site.register(TestAudit)