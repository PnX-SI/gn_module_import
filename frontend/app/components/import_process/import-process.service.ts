import { Injectable } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { Step } from '../../models/enums.model';
import { Import } from '../../models/import.model';
import { ModuleConfig } from "../../module.config";

@Injectable()
export class ImportProcessService {
    private importData: Import | null = null;

constructor(
    private router: Router,
    private route: ActivatedRoute) { }

  setImportData(importData: Import) {
    this.importData  = importData;
  }

  getImportData(): Import | null {
    return this.importData;
  }

  getLastAvailableStep(): Step {
    let lastAvailableStep = Step.Import;
    if (!this.importData.full_file_name) {
      lastAvailableStep = Step.Upload;
    } else if (!this.importData.columns || !this.importData.columns.length) {
      lastAvailableStep = Step.Decode;
    } else if (!this.importData.loaded) {
      lastAvailableStep = Step.FieldMapping;
    } else if (!this.importData.contentmapping) {
      lastAvailableStep = Step.ContentMapping;
    }
    return lastAvailableStep;
  }

  resetImportData() {
    this.importData = null;
  }

  getRouterLinkForStep(step: Step) {
    if (this.importData == null) return null;
    let stepName = Step[step].toLowerCase();
    let importId: number = this.importData.id_import;
    return [ModuleConfig.MODULE_URL, 'process', importId, stepName];
  }

  navigateToStep(step: Step) {
    this.router.navigate(this.getRouterLinkForStep(step));
  }

  // If some steps must be skipped, implement it here
  getPreviousStep(step: Step): Step {
    if (!ModuleConfig.ALLOW_VALUE_MAPPING && step===5) {
        return step - 2;
    }
    return step - 1;
  }

  // If some steps must be skipped, implement it here
  getNextStep(step: Step): Step {
    if (!ModuleConfig.ALLOW_VALUE_MAPPING && step===3) {
        return step + 2;
    }
    return step + 1;
  }

  navigateToPreviousStep(step: Step) {
    this.navigateToStep(this.getPreviousStep(step));
  }

  navigateToNextStep(step: Step) {
    this.navigateToStep(this.getNextStep(step));
  }

  navigateToLastStep() {
    this.navigateToStep(this.getLastAvailableStep());
  }

  beginProcess(datasetId: number) {
    const link = [ModuleConfig.MODULE_URL, 'process', Step[Step.Upload].toLowerCase()];
	   this.router.navigate(link, { queryParams: { datasetId: datasetId } });
  }

  continueProcess(importData: Import) {
    this.importData = importData;
    this.navigateToStep(this.getLastAvailableStep());
  }
}
