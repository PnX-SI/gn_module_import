import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { GN2CommonModule } from '@geonature_common/GN2Common.module';
import { Routes, RouterModule } from '@angular/router';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatStepperModule } from '@angular/material/stepper';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ImportComponent } from './components/import_list/import.component';
import { ImportModalDatasetComponent } from './components/modal_dataset/import-modal-dataset.component';
import { DataService } from './services/data.service';
import { StepsService } from './components/import_process/steps.service';
import { UploadFileStepComponent } from './components/import_process/upload-file-step/upload-file-step.component';
import { FieldsMappingStepComponent } from './components/import_process/fields-mapping-step/fields-mapping-step.component';
import { ContentMappingStepComponent } from './components/import_process/content-mapping-step/content-mapping-step.component';
import { ImportStepComponent } from './components/import_process/import-step/import-step.component';
import { stepperComponent } from './components/import_process/stepper/stepper.component';
import { FooterStepperComponent } from './components/import_process/footer-stepper/footer-stepper.component';

// my module routing
const routes: Routes = [
	{ path: '', component: ImportComponent },
	{
		path: 'process/step/1',
		component: UploadFileStepComponent
	},
	{
		path: 'process/step/2',
		component: FieldsMappingStepComponent
	},
	{
		path: 'process/step/3',
		component: ContentMappingStepComponent
	},
	{
		path: 'process/step/4',
		component: ImportStepComponent
	}
];

@NgModule({
	declarations: [
		ImportComponent,
		ImportModalDatasetComponent,
		UploadFileStepComponent,
		FieldsMappingStepComponent,
		ContentMappingStepComponent,
		ImportStepComponent,
		stepperComponent,
		FooterStepperComponent
	],

	imports: [
		GN2CommonModule,
		RouterModule.forChild(routes),
		CommonModule,
		MatProgressSpinnerModule,
		MatStepperModule,
		MatCheckboxModule
	],

	providers: [ DataService, StepsService ],

	bootstrap: []
})
export class GeonatureModule {}
