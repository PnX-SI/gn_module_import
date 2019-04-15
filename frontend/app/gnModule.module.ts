import { NgModule } from "@angular/core";
import { CommonModule } from '@angular/common';
import { GN2CommonModule } from "@geonature_common/GN2Common.module";
import { Routes, RouterModule } from "@angular/router";
import { ImportComponent } from "./components/import.component";

// my module routing
const routes: Routes = [
  { path: '', component: ImportComponent }
];

@NgModule({
  declarations: [
    ImportComponent
  ],

  imports: [
    GN2CommonModule, 
    RouterModule.forChild(routes), 
    CommonModule
  ],

  entryComponents: [
  ],

  providers: [
  ],

  bootstrap: []
})

export class GeonatureModule {}
