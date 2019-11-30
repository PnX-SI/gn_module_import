import { NgModule } from "@angular/core";
import { CommonModule } from '@angular/common';
import { GN2CommonModule } from "@geonature_common/GN2Common.module";
import { Routes, RouterModule, } from "@angular/router";
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatStepperModule } from '@angular/material/stepper';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ImportComponent } from "./components/import/import.component";
import { ImportProcessComponent } from "./components/import_process/import-process.component";
import { ImportModalDatasetComponent } from "./components/modal_dataset/import-modal-dataset.component";
import { DataService } from "./services/data.service";
import { CsvExportService } from "./services/csv-export.service";
import { StepsService } from './components/import_process/steps.service';
import { UploadFileStepComponent } from "./components/import_process/upload-file-step/upload-file-step.component"
import { FieldsMappingStepComponent } from "./components/import_process/fields-mapping-step/fields-mapping-step.component"
import { ContentMappingStepComponent } from "./components/import_process/content-mapping-step/content-mapping-step.component"
import { ImportStepComponent } from "./components/import_process/import-step/import-step.component"

// my module routing
const routes: Routes = [
  { path: "", component: ImportComponent },
  { path: "process", component: ImportProcessComponent}
];

@NgModule({
  declarations: [
    ImportComponent,
    ImportProcessComponent,
    ImportModalDatasetComponent,
    UploadFileStepComponent,
    FieldsMappingStepComponent,
    ContentMappingStepComponent,
    ImportStepComponent
  ],

  imports: [
    GN2CommonModule, 
    RouterModule.forChild(routes), 
    CommonModule,
    MatProgressSpinnerModule,
    MatStepperModule,
    MatCheckboxModule
  ],

  providers: [
    DataService,
    StepsService,
    CsvExportService
  ],

  bootstrap: []
})

export class GeonatureModule {}
