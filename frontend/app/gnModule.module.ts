import { NgModule } from "@angular/core";
import { CommonModule } from '@angular/common';
import { GN2CommonModule } from "@geonature_common/GN2Common.module";
import { Routes, RouterModule, } from "@angular/router";
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import {MatStepperModule} from '@angular/material/stepper';
import {MatCheckboxModule} from '@angular/material/checkbox';
import { ImportComponent } from "./components/import/import.component";
import { ImportProcessComponent } from "./components/import_process/import-process.component";
import { ImportModalDatasetComponent } from "./components/modal_dataset/import-modal-dataset.component";
import { DataService } from "./services/data.service";

// my module routing
const routes: Routes = [
  { path: "", component: ImportComponent },
  { path: "process", component: ImportProcessComponent}
];

@NgModule({
  declarations: [
    ImportComponent,
    ImportProcessComponent,
    ImportModalDatasetComponent
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
    DataService
  ],

  bootstrap: []
})

export class GeonatureModule {}
