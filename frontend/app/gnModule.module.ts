import { NgModule } from "@angular/core";
import { CommonModule } from "@angular/common";
import { NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { GN2CommonModule } from "@geonature_common/GN2Common.module";
import { Routes, RouterModule } from "@angular/router";
import { MatProgressSpinnerModule } from "@angular/material/progress-spinner";
import { MatStepperModule } from "@angular/material/stepper";
import { MatCheckboxModule } from "@angular/material/checkbox";
import { ChartsModule } from 'ng2-charts';

import { ImportModalDatasetComponent } from "./components/modal_dataset/import-modal-dataset.component";
import { ModalDeleteImport } from "./components/delete-modal/delete-modal.component";
import { DataService } from "./services/data.service";
import { CsvExportService } from "./services/csv-export.service";
import { StepperGuardService } from "./services/stepper-guard";
import { FieldMappingService } from "./services/mappings/field-mapping.service";
import { ContentMappingService } from "./services/mappings/content-mapping.service";
import { ImportComponent } from "./components/import_list/import.component";
import { ImportReportComponent } from "./components/import_report/import_report.component";
import { StepsService } from "./components/import_process/steps.service";
import { UploadFileStepComponent } from "./components/import_process/upload-file-step/upload-file-step.component";
import { FieldsMappingStepComponent } from "./components/import_process/fields-mapping-step/fields-mapping-step.component";
import { ContentMappingStepComponent } from "./components/import_process/content-mapping-step/content-mapping-step.component";
import { ImportStepComponent } from "./components/import_process/import-step/import-step.component";
import { stepperComponent } from "./components/import_process/stepper/stepper.component";
import { FooterStepperComponent } from "./components/import_process/footer-stepper/footer-stepper.component";

// my module routing
const routes: Routes = [
  { path: "", component: ImportComponent },
  { path: "report/:id_import", component: ImportReportComponent },
  {
    path: "process/step/1",
    component: UploadFileStepComponent,
    canActivate: [StepperGuardService]
  },
  {
    path: "process/id_import/:id_import/step/2",
    component: FieldsMappingStepComponent,
    canActivate: [StepperGuardService]
  },
  {
    path: "process/id_import/:id_import/step/3",
    component: ContentMappingStepComponent,
    canActivate: [StepperGuardService]
  },
  {
    path: "process/id_import/:id_import/step/4",
    component: ImportStepComponent,
    canActivate: [StepperGuardService]
  },
  {
    path: "process//id_import/:id_import/step/4",
    component: ImportStepComponent
  }
];

@NgModule({
  declarations: [
    ImportComponent,
    ImportReportComponent,
    ImportModalDatasetComponent,
    ModalDeleteImport,
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
    MatCheckboxModule,
    ChartsModule,
    NgbModule
  ],
  entryComponents: [ModalDeleteImport],
  providers: [
    DataService,
    StepsService,
    CsvExportService,
    FieldMappingService,
    StepperGuardService,
    ContentMappingService
  ],

  bootstrap: []
})
export class GeonatureModule { }
